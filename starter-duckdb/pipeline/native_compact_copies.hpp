#pragma once

#include "native_packed_events.hpp"

namespace shopping::compact_copies {

using packed_events::Event;

inline constexpr uint64_t ordinal_mask = (uint64_t{1} << 40) - 1;
inline constexpr uint64_t key_mask = (uint64_t{1} << 36) - 1;
inline constexpr uint64_t bad_key = UINT64_MAX;
inline constexpr uint64_t ingest_limit = uint64_t{1} << 24;
inline constexpr unsigned producer_bits = 4;
inline constexpr uint32_t producer_mask = (uint32_t{1} << producer_bits) - 1;
inline constexpr uint32_t max_producer_records = UINT32_MAX >> producer_bits;

inline uint64_t base36(std::string_view text, bool upper) {
#if defined(__AVX2__)
    if (!upper && text.size() == 8) {
        uint64_t bytes;
        std::memcpy(&bytes, text.data(), sizeof(bytes));
        const auto input = _mm_cvtsi64_si128(bytes);
        const auto digits = _mm_sub_epi8(input, _mm_set1_epi8('0'));
        const auto letters = _mm_sub_epi8(input, _mm_set1_epi8('a'));
        const auto is_digit = _mm_cmpeq_epi8(digits, _mm_min_epu8(digits, _mm_set1_epi8(9)));
        const auto is_letter = _mm_cmpeq_epi8(letters, _mm_min_epu8(letters, _mm_set1_epi8(25)));
        if ((_mm_movemask_epi8(_mm_or_si128(is_digit, is_letter)) & 255) != 255) {
            return bad_key;
        }
        const auto values = _mm_blendv_epi8(_mm_add_epi8(letters, _mm_set1_epi8(10)), digits, is_digit);
        const auto pairs = _mm_maddubs_epi16(values, _mm_set1_epi16(0x0124));
        const auto quartets = _mm_madd_epi16(pairs, _mm_set1_epi32((1 << 16) | 1296));
        return uint64_t{static_cast<uint32_t>(_mm_cvtsi128_si32(quartets))} * 1'679'616 +
               static_cast<uint32_t>(_mm_extract_epi32(quartets, 1));
    }
#endif
    uint64_t result = 0;
    const char first_letter = upper ? 'A' : 'a';
    for (const unsigned char byte : text) {
        unsigned digit = byte - '0';
        if (digit > 9) {
            const unsigned letter = byte - first_letter;
            if (letter > 25) {
                return bad_key;
            }
            digit = letter + 10;
        }
        result = result * 36 + digit;
    }
    return result;
}

inline uint64_t project_id(uint64_t id) {
    // The odd multiplier is a unit modulo 2^40; xor-right is a bijection.
    // These affect distribution only. The full projected key is compared.
    const auto mixed = (id * 0xa0905881e9ULL) & ordinal_mask;
    return mixed ^ (mixed >> 17) ^ (mixed >> 34);
}

inline uint64_t event_key(std::string_view text) {
    if (text.size() < 2 || text.size() > 9) {
        return bad_key;
    }
    const bool upper = text.front() == 'E';
    if (!upper && text.front() != 'e') {
        return bad_key;
    }
    const auto id = base36(text.substr(1), upper);
    if (id > ordinal_mask) {
        return bad_key;
    }
    const auto shape = static_cast<uint64_t>((text.size() - 2) | (unsigned(upper) << 3));
    return project_id(id) | (shape << 40);
}

struct Copy {
    uint64_t key_and_ingest;
    Event event;

    uint64_t key() const { return key_and_ingest & key_mask; }
};
static_assert(sizeof(Copy) == 24 && partition_count == 256);

using Input = Partitioned<Copy>;

inline std::optional<Input> scan(const fs::path& data, int64_t start, int64_t end,
                                 const IdKeys& user_keys, const Products& products,
                                 unsigned threads) {
    if (threads > producer_mask + 1) {
        return std::nullopt;
    }
    Input copies(threads);
    packed_events::IngestDates dates(start, end);
    const auto base_seconds = start / 1000 - (start % 1000 < 0) - 86'400;
    std::atomic<bool> needs_fallback{false};
    scan_shards<7>(data / "events", threads, [&](const auto& row, unsigned worker) {
        if (row[0].empty() || needs_fallback.load(std::memory_order_relaxed)) {
            return;
        }
        const auto key = event_key(row[0]);
        if (key == bad_key) {
            needs_fallback.store(true, std::memory_order_relaxed);
            return;
        }
        const auto ingest = dates.parse(row[3]);
        if (ingest.seconds == input::invalid_timestamp) {
            needs_fallback.store(true, std::memory_order_relaxed);
            return;
        }
        const auto rank = static_cast<uint64_t>(ingest.seconds - base_seconds);
        auto& owner = copies[worker][key & 255];
        if (rank >= ingest_limit || owner.size() == max_producer_records) {
            needs_fallback.store(true, std::memory_order_relaxed);
            return;
        }
        const auto event = event_payload<uint32_t>(
            row, [&](int64_t time) { return ingest.late(time); }, start, end, start, products,
            [&](std::string_view user) { return user_keys.key(user); });
        // The owner supplies the low eight bits of the exact projected key.
        owner.push_back({(rank << 36) | (key >> 8), event});
    }, [&](const auto& files) {
        reserve_copies(files, copies);
    });

    if (needs_fallback.load(std::memory_order_relaxed)) {
        return std::nullopt;
    }
    return copies;
}

inline packed_events::Input deduplicate(Input& copies, unsigned threads) {
    packed_events::Input result{{}, Partitioned<Event>(threads)};
    size_t total = 0;
    for (const auto& source : copies) {
        for (const auto& bucket : source) {
            total += bucket.size();
        }
    }
    const size_t estimate = total / threads / partition_count;
    if (estimate != 0) {
        const size_t capacity = std::min<size_t>(65'536, estimate + estimate / 4);
        for (auto& output : result.payloads) {
            for (auto& bucket : output) {
                bucket.reserve(capacity);
            }
        }
    }

    parallel_for(partition_count, threads, [&](size_t bucket, unsigned worker) {
        size_t count = 0;
        std::array<Copy*, producer_mask + 1> data{};
        for (size_t producer = 0; producer < copies.size(); ++producer) {
            auto& source = copies[producer][bucket];
            data[producer] = source.data();
            count += source.size();
        }
        if (count == 0) {
            return;
        }

        const auto domain = std::bit_ceil(std::min(count, size_t{1} << 22));
        std::vector<uint32_t> slots(domain);
        std::unordered_map<uint64_t, uint32_t> collisions;
        auto& output = result.payloads[worker];
        auto forward = [&](const Event& event) -> uint64_t {
            if (event.person == 0) {
                return 0;
            }
            const auto owner = bucket_for_key(event.person);
            auto& destination = output[owner];
            destination.push_back(event);
            return (static_cast<uint64_t>(destination.size()) << 8) | owner;
        };
        const auto copy_at = [&](uint32_t handle) {
            return data[handle & producer_mask] + (handle >> producer_bits) - 1;
        };
        const auto discard = [&](uint64_t handle) {
            if (handle != 0) {
                output[handle & 255][(handle >> 8) - 1].person = 0;
            }
        };

        for (size_t producer = 0; producer < copies.size(); ++producer) {
            auto& source = copies[producer][bucket];
            for (size_t index = 0; index < source.size(); ++index) {
                auto& copy = source[index];
                auto* slot = &slots[copy.key() & (domain - 1)];
                const auto handle = static_cast<uint32_t>(((index + 1) << producer_bits) | producer);
                if (*slot == 0) {
                    *slot = handle;
                    copy.event.person = forward(copy.event);
                    continue;
                }

                auto* winner = copy_at(*slot);
                if (winner->key() != copy.key()) {
                    slot = &collisions.try_emplace(copy.key(), 0).first->second;
                    winner = *slot != 0 ? copy_at(*slot) : nullptr;
                }
                if (winner == nullptr || copy.key_and_ingest < winner->key_and_ingest) {
                    if (winner != nullptr) {
                        discard(winner->event.person);
                    }
                    *slot = handle;
                    // The source person is consumed. Keep the destination
                    // index stable across reallocations for replacement.
                    copy.event.person = forward(copy.event);
                }
            }
        }
        for (auto& source : copies) {
            std::vector<Copy>().swap(source[bucket]);
        }
    });
    return result;
}

}  // namespace shopping::compact_copies
