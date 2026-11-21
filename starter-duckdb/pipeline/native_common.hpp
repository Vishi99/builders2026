#pragma once

#include "native_input.hpp"

#include <atomic>
#include <bit>
#include <cstdio>
#include <deque>
#include <exception>
#include <filesystem>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <type_traits>
#include <unordered_map>
#include <vector>

#if defined(__SSE2__)
#include <emmintrin.h>
#endif

namespace shopping {

namespace fs = std::filesystem;

inline constexpr int64_t day_ms = 86'400'000;
inline constexpr int64_t week_ms = 7 * day_ms;
inline constexpr size_t partition_count = 256;

class StageTimer {
public:
    void report(const char* stage) {
        const auto end = std::chrono::steady_clock::now();
        const std::chrono::duration<double> elapsed = end - start_;
        std::fprintf(stderr, "%s: %.3fs\n", stage, elapsed.count());
        start_ = end;
    }

private:
    std::chrono::steady_clock::time_point start_ = std::chrono::steady_clock::now();
};

inline int32_t day_number(int64_t ts) {
    return static_cast<int32_t>(ts / day_ms - (ts % day_ms < 0));
}

inline uint64_t hash_id(uint64_t key) {
    key ^= key >> 33;
    key *= 0xff51afd7ed558ccdULL;
    key ^= key >> 33;
    key *= 0xc4ceb9fe1a85ec53ULL;
    return key ^ (key >> 33);
}

inline size_t bucket_for_key(uint64_t key) {
    return (hash_id(key) >> 32) % partition_count;
}

template<class Work>
void parallel_for(size_t count, unsigned threads, Work&& work) {
    threads = std::min<size_t>(threads, count);
    if (threads <= 1) {
        for (size_t index = 0; index < count; ++index) {
            work(index, 0);
        }
        return;
    }

    std::atomic<size_t> next{0};
    std::mutex failure_mutex;
    std::exception_ptr failure;
    auto run = [&](unsigned worker) {
        try {
            for (size_t index; (index = next.fetch_add(1, std::memory_order_relaxed)) < count;) {
                work(index, worker);
            }
        } catch (...) {
            next.store(count, std::memory_order_relaxed);
            std::lock_guard lock(failure_mutex);
            if (!failure) {
                failure = std::current_exception();
            }
        }
    };

    {
        std::vector<std::jthread> workers;
        workers.reserve(threads - 1);
        for (unsigned worker = 1; worker < threads; ++worker) {
            workers.emplace_back(run, worker);
        }
        run(0);
    }
    if (failure) {
        std::rethrow_exception(failure);
    }
}

template<size_t Width, class OnRow, class Prepare = std::nullptr_t>
void scan_shards(const fs::path& folder, unsigned threads, OnRow&& on_row, Prepare prepare = nullptr) {
    constexpr size_t chunk_bytes = 4 << 20;
    struct Chunk {
        size_t file, begin, end;
    };
    std::vector<input::BufferedFile> files;
    std::vector<Chunk> chunks;
    if (!fs::exists(folder)) {
        return;
    }
    for (const auto& entry : fs::directory_iterator(folder)) {
        if (!entry.path().filename().native().ends_with(".csv")) {
            continue;
        }
        const size_t index = files.size();
        files.emplace_back(entry.path().c_str());
        const size_t size = files.back().size();
        for (size_t begin = 0; begin < size; begin += chunk_bytes) {
            chunks.push_back({index, begin, std::min(size, begin + chunk_bytes)});
        }
    }
    if constexpr (!std::is_same_v<Prepare, std::nullptr_t>) {
        prepare(files);
    }
    std::vector<input::ChunkBuffer> buffers(std::min<size_t>(threads, chunks.size()));
    parallel_for(chunks.size(), threads, [&](size_t index, unsigned worker) {
        const auto [file, begin, end] = chunks[index];
        input::for_each_row<Width>(files[file], buffers[worker], begin, end, [&](const auto& row) {
            on_row(row, worker);
        });
    });
}

struct StringHash {
    using is_transparent = void;
    size_t operator()(std::string_view key) const noexcept {
        return std::hash<std::string_view>{}(key);
    }
};

using StringIds = std::unordered_map<std::string, uint32_t, StringHash, std::equal_to<>>;

// Short ASCII IDs fit in a word whose order matches their byte order.
// All other IDs are interned exactly in a separate key space.
class IdKeys {
    static constexpr uint64_t interned = 1ULL << 63;

public:
    uint64_t key(std::string_view text) const {
        if (text.size() > 9) {
            return intern(text);
        }
#if defined(__AVX2__) && defined(__BMI2__)
        if (text.size() >= 8) {
            uint64_t prefix;
            std::memcpy(&prefix, text.data(), sizeof(prefix));
            const auto last = text.size() == 9 ? static_cast<unsigned char>(text.back()) : 0;
            constexpr uint64_t high_bits = 0x8080808080808080ULL;
            constexpr uint64_t byte_ones = 0x0101010101010101ULL;
            // A set high bit after subtracting one flags an ASCII NUL byte.
            if (((prefix | (prefix - byte_ones)) & high_bits) == 0 &&
                (text.size() == 8 || (last > 0 && last < 128))) {
                const uint64_t packed = _pext_u64(__builtin_bswap64(prefix), ~high_bits);
                return (packed << 7) | last;
            }
            return intern(text);
        }
#endif
        uint64_t value = 0;
        for (unsigned char byte : text) {
            if (byte == 0 || byte >= 128) {
                return intern(text);
            }
            value = (value << 7) | byte;
        }
        return value << ((9 - text.size()) * 7);
    }

    void decode(uint64_t key, std::string& text) const {
        if (key & interned) {
            std::lock_guard lock(mutex_);
            text.assign(names_[key & ~interned]);
            return;
        }
        char buffer[9];
        size_t length = 0;
        for (int shift = 56; shift >= 0; shift -= 7) {
            const auto digit = (key >> shift) & 127;
            if (digit == 0) {
                break;
            }
            buffer[length++] = static_cast<char>(digit);
        }
        text.assign(buffer, length);
    }

    bool less(uint64_t a, uint64_t b) const {
        if (((a | b) & interned) == 0) {
            return a < b;
        }
        std::string left, right;
        decode(a, left);
        decode(b, right);
        return left < right;
    }

private:
    uint64_t intern(std::string_view text) const {
        std::lock_guard lock(mutex_);
        if (const auto it = keys_.find(text); it != keys_.end()) {
            return it->second;
        }
        const uint64_t key = interned | names_.size();
        keys_.emplace(names_.emplace_back(text), key);
        return key;
    }

    mutable std::mutex mutex_;
    mutable std::deque<std::string> names_;
    mutable std::unordered_map<std::string_view, uint64_t, StringHash> keys_;
};

class IdMap {
    struct Entry {
        uint64_t key = 0;
        uint64_t value = 0;
    };

public:
    uint64_t get(uint64_t key, uint64_t hash) const {
#if defined(__SSE2__)
        if (!fingerprints_.empty()) {
            return get_by_fingerprint(key, hash);
        }
#endif
        if (!bloom_.empty()) {
            const auto bits = bloom_bits(hash);
            if ((bloom_[hash & (bloom_.size() - 1)] & bits) != bits) {
                return 0;
            }
        }
        return entries_[slot(key, hash)].value;
    }

    void reserve(size_t count) {
        entries_.resize(std::max<size_t>(16, std::bit_ceil(count + count / 2 + 1)));
    }

    // Alias shards have a known number of distinct keys and become read-only
    // once built. The union-find index still uses the ordinary mutable path.
    void reserve_aliases(size_t count) {
        reserve(count);
        if (count == 0) {
            return;
        }
#if defined(__SSE2__)
        // In the local benchmarks the Bloom precheck worked best on 128 KiB
        // shards, while control-byte probes won on the larger shards.
        constexpr size_t bloom_shard_bytes = 128 * 1024;
        if (entries_.size() > bloom_shard_bytes / sizeof(Entry)) {
            fingerprints_.resize(entries_.size() + probe_width - 1);
            return;
        }
#endif
        bloom_.resize(std::bit_ceil((count + 7) / 8));
    }

    void insert_alias(uint64_t key, uint64_t person, uint64_t hash) {
        const auto index = slot(key, hash);
        entries_[index] = {key, person};
#if defined(__SSE2__)
        if (!fingerprints_.empty()) {
            const auto tag = fingerprint(hash);
            fingerprints_[index] = tag;
            // Mirror the start of the ring so every 16-byte load is in bounds.
            if (index < probe_width - 1) {
                fingerprints_[entries_.size() + index] = tag;
            }
            return;
        }
#endif
        bloom_[hash & (bloom_.size() - 1)] |= bloom_bits(hash);
    }

    template<class NewValue>
    uint64_t get_or_insert(uint64_t key, NewValue&& make_value) {
        const uint64_t hash = hash_id(key);
        size_t index = slot(key, hash);
        if (entries_[index].key != 0) {
            return entries_[index].value;
        }
        if ((size_ + 1) * 3 >= entries_.size() * 2) {
            auto old = std::move(entries_);
            entries_.resize(old.size() * 2);
            for (const auto& entry : old) {
                if (entry.key != 0) {
                    entries_[slot(entry.key, hash_id(entry.key))] = entry;
                }
            }
            index = slot(key, hash);
        }
        const auto value = make_value();
        entries_[index] = {key, value};
        ++size_;
        return value;
    }

    template<class Visit>
    void for_each(Visit&& visit) const {
        for (const auto& entry : entries_) {
            if (entry.key != 0) {
                visit(entry.key, entry.value);
            }
        }
    }

private:
    static uint64_t bloom_bits(uint64_t hash) {
        // All four probes share one word. Avoid bits 32–39, which are fixed
        // within the alias shard.
        return (uint64_t{1} << ((hash >> 40) & 63)) |
               (uint64_t{1} << ((hash >> 46) & 63)) |
               (uint64_t{1} << ((hash >> 52) & 63)) |
               (uint64_t{1} << (hash >> 58));
    }

#if defined(__SSE2__)
    static constexpr size_t probe_width = 16;

    static uint8_t fingerprint(uint64_t hash) {
        return static_cast<uint8_t>((hash >> 56) | 0x80);
    }

    uint64_t get_by_fingerprint(uint64_t key, uint64_t hash) const {
        const size_t mask = entries_.size() - 1;
        const auto expected = _mm_set1_epi8(static_cast<char>(fingerprint(hash)));
        size_t index = hash & mask;
        for (;;) {
            const auto group = _mm_loadu_si128(
                reinterpret_cast<const __m128i*>(fingerprints_.data() + index));
            const unsigned empty = _mm_movemask_epi8(_mm_cmpeq_epi8(group, _mm_setzero_si128()));
            unsigned matches = _mm_movemask_epi8(_mm_cmpeq_epi8(group, expected));
            if (empty != 0) {
                matches &= (1u << std::countr_zero(empty)) - 1;
            }
            while (matches != 0) {
                const auto& entry = entries_[(index + std::countr_zero(matches)) & mask];
                if (entry.key == key) {
                    return entry.value;
                }
                matches &= matches - 1;
            }
            if (empty != 0) {
                return 0;
            }
            index = (index + probe_width) & mask;
        }
    }
#endif

    size_t slot(uint64_t key, uint64_t hash) const {
        const size_t mask = entries_.size() - 1;
        size_t index = hash & mask;
        while (entries_[index].key != 0 && entries_[index].key != key) {
            index = (index + 1) & mask;
        }
        return index;
    }

    std::vector<Entry> entries_ = std::vector<Entry>(16);
    size_t size_ = 0;
    std::vector<uint64_t> bloom_;
#if defined(__SSE2__)
    std::vector<uint8_t> fingerprints_;
#endif
};

class Identities {
    struct Node {
        uint64_t minimum;
        uint32_t parent;
        uint32_t size;
    };

public:
    struct alignas(64) LookupCache {
        struct Entry {
            uint64_t key = 0;
            uint64_t person = 0;
        };
        std::array<Entry, 512> entries;
    };

    Identities(const fs::path& links, const IdKeys& keys, unsigned threads) : keys_(keys) {
        using Edge = std::pair<uint64_t, uint64_t>;
        std::vector<std::vector<Edge>> edges(threads);
        scan_shards<2>(links, threads, [&](const auto& row, unsigned worker) {
            if (!row[0].empty() && !row[1].empty() && row[0] != row[1]) {
                edges[worker].emplace_back(keys_.key(row[0]), keys_.key(row[1]));
            }
        });

        size_t count = 0;
        for (const auto& shard : edges) {
            count += shard.size();
        }
        IdMap linked_nodes;
        linked_nodes.reserve(count);
        std::vector<Node> nodes;
        nodes.reserve(count);
        auto node_for = [&](uint64_t key) {
            return static_cast<uint32_t>(linked_nodes.get_or_insert(key, [&] {
                const auto at = static_cast<uint32_t>(nodes.size());
                nodes.push_back({key, at, 1});
                return uint64_t{at} + 1;
            }) - 1);
        };
        auto root = [&](uint32_t at) {
            while (nodes[at].parent != at) {
                nodes[at].parent = nodes[nodes[at].parent].parent;
                at = nodes[at].parent;
            }
            return at;
        };

        for (const auto& shard : edges) {
            for (const auto& [left, right] : shard) {
                auto a = root(node_for(left));
                auto b = root(node_for(right));
                if (a == b) {
                    continue;
                }
                if (nodes[a].size < nodes[b].size) {
                    std::swap(a, b);
                }
                nodes[b].parent = a;
                nodes[a].size += nodes[b].size;
                if (keys_.less(nodes[b].minimum, nodes[a].minimum)) {
                    nodes[a].minimum = nodes[b].minimum;
                }
            }
        }
        for (size_t index = 0; index < nodes.size(); ++index) {
            nodes[index].minimum = nodes[root(index)].minimum;
        }
        // The read-side map only needs IDs that actually change. Shard it
        // using the same hash bits as the raw event partitions.
        std::array<size_t, partition_count> sizes{};
        linked_nodes.for_each([&](uint64_t key, uint64_t index) {
            if (key != nodes[index - 1].minimum) {
                ++sizes[bucket_for_key(key)];
            }
        });
        for (size_t bucket = 0; bucket < partition_count; ++bucket) {
            aliases_[bucket].reserve_aliases(sizes[bucket]);
        }
        linked_nodes.for_each([&](uint64_t key, uint64_t index) {
            const auto person = nodes[index - 1].minimum;
            if (key != person) {
                const uint64_t hash = hash_id(key);
                aliases_[(hash >> 32) % partition_count].insert_alias(key, person, hash);
            }
        });
    }

    uint64_t key(std::string_view user) const {
        return keys_.key(user);
    }

    uint64_t canonical(uint64_t key) const {
        return canonical_key(key, hash_id(key));
    }

    uint64_t person(std::string_view user) const {
        return canonical(key(user));
    }

    uint64_t person(std::string_view user, LookupCache& cache) const {
        const uint64_t key = keys_.key(user);
        const uint64_t hash = hash_id(key);
        auto& entry = cache.entries[hash % cache.entries.size()];
        if (entry.key != key) {
            entry = {key, canonical_key(key, hash)};
        }
        return entry.person;
    }

private:
    uint64_t canonical_key(uint64_t key, uint64_t hash) const {
        const uint64_t canonical = aliases_[(hash >> 32) % partition_count].get(key, hash);
        return canonical == 0 ? key : canonical;
    }

    const IdKeys& keys_;
    std::array<IdMap, partition_count> aliases_;
};

}  // namespace shopping
