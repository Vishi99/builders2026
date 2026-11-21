#pragma once

#include "native_common.hpp"
#include "native_sessions.hpp"

#include <bit>
#include <memory>
#include <type_traits>

#if defined(__SSE2__)
#include <emmintrin.h>
#endif

namespace shopping {

struct Session {
    uint64_t person;
    int64_t start_ms;
    int64_t revenue;
};

struct UserDay {
    uint64_t person;
    int64_t revenue;
    int32_t day;
    uint32_t sessions;
    uint32_t events;
    uint32_t category;
};

struct Products {
    std::vector<std::string> categories;
    StringIds ids;
    uint32_t unknown;

    explicit Products(const fs::path& path) {
        input::MappedFile file(path.c_str());
        std::vector<std::pair<std::string_view, std::string_view>> products;
        categories = {"unknown"};
        std::unordered_map<std::string_view, uint32_t, StringHash> ranks;
        ranks.emplace("unknown", 0);
        input::for_each_row<2>(file, [&](const auto& row) {
            const auto category = row[1].empty() ? std::string_view("unknown") : row[1];
            if (ranks.try_emplace(category, 0).second) {
                categories.emplace_back(category);
            }
            products.emplace_back(row[0], category);
        });
        std::sort(categories.begin(), categories.end());
        // Index zero represents a day with no purchases.
        categories.insert(categories.begin(), std::string());
        for (size_t i = 1; i < categories.size(); ++i) {
            ranks.find(categories[i])->second = i;
        }
        unknown = ranks.at("unknown");
        ids.reserve(products.size());
        for (const auto& [product, category] : products) {
            ids.emplace(product, ranks.at(category));
        }
    }

    uint32_t category(std::string_view product) const {
        const auto found = ids.find(product);
        return found == ids.end() ? unknown : found->second;
    }
};

template<class Time>
struct EventRecord {
    using Purchase = std::make_unsigned_t<Time>;
    static constexpr unsigned price_bits = sizeof(Time) == 4 ? 24 : 32;
    static constexpr Purchase price_mask = (Purchase{1} << price_bits) - 1;

    uint64_t person = 0;
    Time timestamp = 0;
    Purchase purchase = 0;

    uint32_t price() const { return purchase & price_mask; }
    uint32_t category() const { return purchase >> price_bits; }

    static EventRecord encode(uint64_t person, int64_t time, uint32_t price, uint32_t category) {
        return {person, static_cast<Time>(time), (Purchase{category} << price_bits) | price};
    }
};

template<class Event>
struct EventCopy {
    uint64_t id;
    int64_t ingest;
    Event event;
};

struct EventTotals {
    std::vector<std::vector<Session>> sessions{partition_count};
    std::vector<std::vector<UserDay>> days{partition_count};
};

template<class Time, class Person>
EventRecord<Time> event_payload(const std::array<std::string_view, 7>& row, int64_t ingest,
                                int64_t start, int64_t end, int64_t time_base,
                                const Products& products, Person&& person) {
    const bool purchase = row[4] == "purchase";
    if (row[1].empty() || row[5].empty() ||
        (!purchase && row[4] != "view" && row[4] != "cart")) {
        return {};
    }
    const int64_t time = input::timestamp_ms(row[2]);
    if (time < start || time >= end || time < ingest - day_ms) {
        return {};
    }
    const uint32_t price = purchase ? input::purchase_price(row[6]) : 0;
    if (purchase && price == 0) {
        return {};
    }
    return EventRecord<Time>::encode(person(row[1]), time - time_base, price,
                                     purchase ? products.category(row[5]) : 0);
}

template<class Event>
void summarize_events(std::vector<Event>& events, int64_t time_base, size_t category_count,
                      std::vector<Session>& sessions, std::vector<UserDay>& days) {
    std::sort(events.begin(), events.end(), [](const Event& a, const Event& b) {
        return a.person < b.person || (a.person == b.person && a.timestamp < b.timestamp);
    });

    CategoryRevenue category_revenue(category_count);
    size_t index = 0;
    while (index < events.size()) {
        const auto person = events[index].person;
        const auto day = day_number(time_base + events[index].timestamp);
        UserDay totals{person, 0, day, 0, 0, 0};
        do {
            Session session{person, time_base + events[index].timestamp, 0};
            const size_t begin = index;
            int64_t previous;
            do {
                const Event& event = events[index++];
                previous = event.timestamp;
                if (const uint32_t price = event.price()) {
                    session.revenue += price;
                    category_revenue.add(event.category(), price);
                }
            } while (index < events.size() && events[index].person == person &&
                     events[index].timestamp - previous <= session_gap_ms);
            sessions.push_back(session);
            ++totals.sessions;
            totals.events += index - begin;
            totals.revenue += session.revenue;
        } while (index < events.size() && events[index].person == person &&
                 day_number(time_base + events[index].timestamp) == day);

        totals.category = category_revenue.winner();
        days.push_back(totals);
    }
}

template<class Record>
using Partitioned = std::vector<std::array<std::vector<Record>, partition_count>>;

class EventWinnerSlots {
    static constexpr size_t group_size = 16;

public:
    explicit EventWinnerSlots(size_t count)
        : tags_(std::max(group_size, std::bit_ceil(count + count / 2 + 1)), 0),
          winners_(std::make_unique_for_overwrite<uintptr_t[]>(tags_.size())) {}

    template<class IsSameKey>
    uintptr_t& find(uint64_t hash, IsSameKey&& same_key) {
        const auto tag = static_cast<uint8_t>((hash >> 56) | 0x80);
        const size_t mask = tags_.size() - 1;
        size_t group = (hash & mask) & ~(group_size - 1);
        for (;;) {
            auto [matches, empty] = scan_group(tags_.data() + group, tag);
            while (matches != 0) {
                auto& winner = winners_[group + std::countr_zero(matches)];
                if (same_key(winner)) {
                    return winner;
                }
                matches &= matches - 1;
            }
            if (empty != 0) {
                const size_t slot = group + std::countr_zero(empty);
                tags_[slot] = tag;
                winners_[slot] = 0;
                return winners_[slot];
            }
            group = (group + group_size) & mask;
        }
    }

private:
    static std::pair<unsigned, unsigned> scan_group(const uint8_t* tags, uint8_t tag) {
#if defined(__SSE2__)
        const auto group = _mm_loadu_si128(reinterpret_cast<const __m128i*>(tags));
        return {static_cast<unsigned>(_mm_movemask_epi8(_mm_cmpeq_epi8(group, _mm_set1_epi8(tag)))),
                static_cast<unsigned>(_mm_movemask_epi8(_mm_cmpeq_epi8(group, _mm_setzero_si128())))};
#else
        unsigned matches = 0;
        unsigned empty = 0;
        for (size_t index = 0; index < group_size; ++index) {
            matches |= unsigned(tags[index] == tag) << index;
            empty |= unsigned(tags[index] == 0) << index;
        }
        return {matches, empty};
#endif
    }

    std::vector<uint8_t> tags_;
    std::unique_ptr<uintptr_t[]> winners_;
};

template<class Copy>
void reserve_copies(const std::vector<input::MappedFile>& files, Partitioned<Copy>& copies) {
    size_t input_bytes = 0;
    size_t sample_bytes = 0;
    size_t sample_rows = 0;
    for (const auto& file : files) {
        input_bytes += file.size();
        if (sample_rows >= 512 || file.size() <= 4096) {
            continue;
        }
        const char* const end = file.data() + std::min<size_t>(file.size(), 65'536);
        for (auto* line = input::detail::next_line(file.data(), end); line < end;) {
            const auto* newline = input::detail::find_byte(line, end, '\n');
            if (newline == nullptr) {
                break;
            }
            sample_bytes += newline + 1 - line;
            ++sample_rows;
            line = newline + 1;
        }
    }
    if (sample_rows == 0) {
        return;
    }

    // This is only an allocation hint. Long rows and uneven shards can still
    // make a bucket grow as usual.
    const size_t row_bytes = std::max<size_t>(32, sample_bytes / sample_rows);
    const size_t estimate = input_bytes / row_bytes / copies.size() / partition_count;
    const size_t capacity = std::min<size_t>(32'768, estimate + estimate / 5);
    for (auto& worker : copies) {
        for (auto& bucket : worker) {
            bucket.reserve(capacity);
        }
    }
}

template<class Time>
EventTotals process_event_records(const fs::path& data, int64_t start, int64_t end,
                                  const Identities& identities, const Products& products,
                                  unsigned threads, StageTimer& timer) {
    using Event = EventRecord<Time>;
    using Copy = EventCopy<Event>;
    const int64_t time_base = sizeof(Time) == 4 ? start : 0;
    IdKeys event_keys;
    Partitioned<Copy> copies(threads);
    std::vector<Identities::LookupCache> people(threads);
    scan_shards<7>(data / "events", threads, [&](const auto& row, unsigned worker) {
        if (row[0].empty()) {
            return;
        }

        const uint64_t id = event_keys.key(row[0]);
        const int64_t ingest = input::ingest_ms(row[3]);
        if (ingest == input::invalid_timestamp) {
            throw std::runtime_error("Invalid ingest_ts on a parsed event");
        }
        auto& partition = copies[worker][bucket_for_key(id)];
        const auto event = event_payload<Time>(row, ingest, start, end, time_base, products,
            [&](std::string_view user) { return identities.person(user, people[worker]); });
        // Even an invalid copy can win deduplication and suppress later copies.
        partition.push_back({id, ingest, event});
    }, [&](const auto& files) {
        reserve_copies(files, copies);
    });
    timer.report("Parse events");

    Partitioned<Event> by_person(threads);
    parallel_for(partition_count, threads, [&](size_t bucket, unsigned worker) {
        size_t count = 0;
        for (const auto& source : copies) {
            count += source[bucket].size();
        }
        if (count == 0) {
            return;
        }
        EventWinnerSlots winners(count);
        for (auto& source : copies) {
            for (auto& copy : source[bucket]) {
                auto& link = winners.find(hash_id(copy.id), [&](uintptr_t candidate) {
                    return reinterpret_cast<Copy*>(candidate)->id == copy.id;
                });
                auto* winner = reinterpret_cast<Copy*>(link);
                if (winner == nullptr || copy.ingest < winner->ingest) {
                    if (winner != nullptr) {
                        winner->event.person = 0;
                    }
                    link = reinterpret_cast<uintptr_t>(&copy);
                } else {
                    copy.event.person = 0;
                }
            }
        }
        // The table compares IDs and ingest times only. Marked losers can be
        // skipped on a sequential pass over the source records.
        for (auto& source : copies) {
            for (const auto& copy : source[bucket]) {
                if (copy.event.person != 0) {
                    by_person[worker][bucket_for_key(copy.event.person)].push_back(copy.event);
                }
            }
            std::vector<Copy>().swap(source[bucket]);
        }
    });
    timer.report("Deduplicate events");

    std::array<std::pair<size_t, size_t>, partition_count> work{};
    for (size_t bucket = 0; bucket < partition_count; ++bucket) {
        auto& [count, index] = work[bucket];
        for (const auto& source : by_person) {
            count += source[bucket].size();
        }
        index = bucket;
    }
    // Start the largest partitions first to avoid leaving a hot user on the
    // critical path after the smaller partitions have finished.
    std::sort(work.begin(), work.end(), std::greater<>());

    EventTotals totals;
    parallel_for(partition_count, threads, [&](size_t task, unsigned) {
        const auto [count, bucket] = work[task];
        if (count == 0) {
            return;
        }
        if constexpr (sizeof(Time) == 4) {
            if (count < std::numeric_limits<uint32_t>::max()) {
                session_bins::Bins bins(count);
                for (auto& source : by_person) {
                    auto& partition = source[bucket];
                    for (const auto& event : partition) {
                        bins.add(event.person, event.timestamp, event.price(), event.category());
                    }
                    std::vector<Event>().swap(partition);
                }
                session_bins::summarize(bins, time_base, products.categories.size(),
                                        totals.sessions[bucket], totals.days[bucket]);
                return;
            }
        }

        std::vector<Event> events;
        events.reserve(count);
        for (auto& source : by_person) {
            auto& partition = source[bucket];
            events.insert(events.end(), partition.begin(), partition.end());
            std::vector<Event>().swap(partition);
        }
        summarize_events(events, time_base, products.categories.size(), totals.sessions[bucket],
                         totals.days[bucket]);
    });
    timer.report("Build sessions and summarize user days");
    return totals;
}

inline bool compact_events_fit(int64_t start, int64_t end, const Products& products) {
    // Only select the compact layout when every timestamp and category can
    // be represented exactly. The price limit fits in 24 bits in both cases.
    const auto span = static_cast<uint64_t>(end) - static_cast<uint64_t>(start);
    return span <= (uint64_t{1} << 32) && products.categories.size() <= 256;
}

inline EventTotals process_events(const fs::path& data, int64_t start, int64_t end,
                                  const Identities& identities, const Products& products,
                                  unsigned threads, StageTimer& timer) {
    if (compact_events_fit(start, end, products)) {
        return process_event_records<uint32_t>(data, start, end, identities, products, threads, timer);
    }
    return process_event_records<int64_t>(data, start, end, identities, products, threads, timer);
}

}  // namespace shopping
