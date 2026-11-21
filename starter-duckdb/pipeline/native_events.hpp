#pragma once

#include "native_common.hpp"

#include <bit>

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

struct Event {
    uint64_t person = 0;
    int64_t timestamp = 0;
    uint32_t price = 0;
    uint32_t category = 0;
};

struct EventCopy {
    uint64_t id;
    int64_t ingest;
    Event event;
};

struct EventTotals {
    std::vector<std::vector<Session>> sessions{partition_count};
    std::vector<std::vector<UserDay>> days{partition_count};
};

inline void summarize_events(std::vector<Event>& events, size_t category_count,
                             std::vector<Session>& sessions, std::vector<UserDay>& days) {
    std::sort(events.begin(), events.end(), [](const Event& a, const Event& b) {
        return a.person < b.person || (a.person == b.person && a.timestamp < b.timestamp);
    });

    std::vector<int64_t> category_revenue(category_count);
    std::vector<uint32_t> purchased_categories;
    size_t index = 0;
    while (index < events.size()) {
        const auto person = events[index].person;
        const auto day = day_number(events[index].timestamp);
        UserDay totals{person, 0, day, 0, 0, 0};
        do {
            Session session{person, events[index].timestamp, 0};
            const size_t begin = index;
            int64_t previous;
            do {
                const Event& event = events[index++];
                previous = event.timestamp;
                if (event.price != 0) {
                    session.revenue += event.price;
                    auto& revenue = category_revenue[event.category];
                    if (revenue == 0) {
                        purchased_categories.push_back(event.category);
                    }
                    revenue += event.price;
                }
            } while (index < events.size() && events[index].person == person &&
                     events[index].timestamp - previous <= 1'800'000);
            sessions.push_back(session);
            ++totals.sessions;
            totals.events += index - begin;
            totals.revenue += session.revenue;
        } while (index < events.size() && events[index].person == person &&
                 day_number(events[index].timestamp) == day);

        int64_t best_revenue = 0;
        for (const uint32_t category : purchased_categories) {
            auto& revenue = category_revenue[category];
            if (revenue > best_revenue || (revenue == best_revenue && category < totals.category)) {
                totals.category = category;
                best_revenue = revenue;
            }
            revenue = 0;
        }
        purchased_categories.clear();
        days.push_back(totals);
    }
}

template<class Record>
using Partitioned = std::vector<std::array<std::vector<Record>, partition_count>>;

inline EventTotals process_events(const fs::path& data, int64_t start, int64_t end,
                                  const Identities& identities, const Products& products,
                                  unsigned threads, StageTimer& timer) {
    IdKeys event_keys;
    Partitioned<EventCopy> copies(threads);
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
        Event event;
        const bool purchase = row[4] == "purchase";
        if (!row[1].empty() && !row[5].empty() &&
            (purchase || row[4] == "view" || row[4] == "cart")) {
            const int64_t time = input::timestamp_ms(row[2]);
            const uint32_t price = purchase ? input::purchase_price(row[6]) : 0;
            if (time >= start && time < end && time >= ingest - day_ms &&
                (!purchase || price != 0)) {
                event = {identities.person(row[1]), time, price,
                         purchase ? products.category(row[5]) : 0};
            }
        }
        // Even an invalid copy can win deduplication and suppress later copies.
        partition.push_back({id, ingest, event});
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
        const size_t capacity = std::bit_ceil(count + count / 2 + 1);
        const size_t mask = capacity - 1;
        std::vector<const EventCopy*> winners(capacity, nullptr);
        for (const auto& source : copies) {
            for (const auto& copy : source[bucket]) {
                size_t slot = hash_id(copy.id) & mask;
                while (winners[slot] != nullptr && winners[slot]->id != copy.id) {
                    slot = (slot + 1) & mask;
                }
                if (winners[slot] == nullptr || copy.ingest < winners[slot]->ingest) {
                    winners[slot] = &copy;
                }
            }
        }
        for (const EventCopy* winner : winners) {
            if (winner != nullptr && winner->event.person != 0) {
                by_person[worker][bucket_for_key(winner->event.person)].push_back(winner->event);
            }
        }
        for (auto& source : copies) {
            std::vector<EventCopy>().swap(source[bucket]);
        }
    });
    timer.report("Deduplicate events");

    EventTotals totals;
    parallel_for(partition_count, threads, [&](size_t bucket, unsigned) {
        size_t count = 0;
        for (const auto& source : by_person) {
            count += source[bucket].size();
        }
        std::vector<Event> events;
        events.reserve(count);
        for (auto& source : by_person) {
            auto& partition = source[bucket];
            events.insert(events.end(), partition.begin(), partition.end());
            std::vector<Event>().swap(partition);
        }
        summarize_events(events, products.categories.size(), totals.sessions[bucket],
                         totals.days[bucket]);
    });
    timer.report("Build sessions and summarize user days");
    return totals;
}

}  // namespace shopping
