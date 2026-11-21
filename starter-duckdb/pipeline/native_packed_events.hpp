#pragma once

#include "native_attribution.hpp"

#include <chrono>
#include <cstdio>
#include <cstring>
#include <iterator>
#include <optional>

namespace shopping {

namespace packed_events {

using Event = EventRecord<uint32_t>;
using CompactClick = attribution::ClickRecord<uint32_t>;

// A ticket's partition supplies bits 32–39 of the reversible event-ID hash.
// The remaining bits are an exact key within that partition. The 39-bit
// ingest field covers every whole UTC second in the four-digit year range.
struct alignas(16) Ticket {
    static constexpr uint64_t key_mask = (uint64_t{1} << 56) - 1;
    static constexpr uint64_t low_hash = (uint64_t{1} << 32) - 1;
    static constexpr int64_t ingest_bias = int64_t{1} << 38;
    static constexpr uint32_t highest_index = UINT32_MAX >> 8;

    uint64_t key_and_ingest;
    uint64_t ingest_and_handle;

    static Ticket encode(uint64_t hash, uint64_t ingest, uint32_t handle) {
        const uint64_t key = (hash & low_hash) | ((hash >> 8) & (key_mask ^ low_hash));
        return {key | (ingest << 56), ((ingest >> 8) << 32) | handle};
    }

    uint64_t key() const { return key_and_ingest & key_mask; }
    uint64_t ingest() const { return ((ingest_and_handle >> 32) << 8) | (key_and_ingest >> 56); }
    uint32_t handle() const { return static_cast<uint32_t>(ingest_and_handle); }

    uint64_t hash(size_t bucket) const {
        return (key_and_ingest & low_hash) | (static_cast<uint64_t>(bucket) << 32) |
               ((key_and_ingest & (key_mask ^ low_hash)) << 8);
    }
};

static_assert(sizeof(Ticket) == 16 && alignof(Ticket) == 16);
static_assert(sizeof(Event) == 16 && partition_count == 256);

struct Input {
    Partitioned<Ticket> tickets;
    Partitioned<Event> payloads;
};

struct Ingest {
    int64_t day_start;
    int64_t seconds;

    bool late(int64_t event) const {
        // Ingest on the event's UTC day or earlier always passes. Only
        // events on the previous UTC day need an exact clock comparison.
        return event < day_start &&
               (event < day_start - day_ms || event < (seconds - 86'400) * 1000);
    }
};

class IngestDates {
    struct Day {
        std::array<char, 11> ascii{};
        int64_t midnight_ms{};
        int64_t midnight_seconds{};
    };
    std::array<Day, 32> dates_{};

    static unsigned slot(std::string_view value) {
        return (static_cast<unsigned char>(value[8]) * 10u +
                static_cast<unsigned char>(value[9])) & 31u;
    }

public:
    IngestDates(int64_t start, int64_t end) {
        using namespace std::chrono;
        const auto day_index = [](int64_t ms) { return ms / day_ms - (ms % day_ms < 0); };
        constexpr auto first_civil_day = sys_days{year{0} / 1 / 1}.time_since_epoch().count();
        constexpr auto last_civil_day = sys_days{year{9999} / 12 / 31}.time_since_epoch().count();
        const auto first = day_index(start);
        if (first < first_civil_day || first > last_civil_day) {
            return;
        }
        // Cache dates around the dataset's window. A miss always falls back
        // to the general parser, including a collision on day-of-month.
        const auto final = std::min<int64_t>({day_index(end - 1) + 7, first + 29, last_civil_day});
        for (int64_t day = first; day <= final; ++day) {
            const auto civil = year_month_day{sys_days{days{static_cast<days::rep>(day)}}};
            const auto year = static_cast<int>(civil.year());
            std::array<char, 11> ascii{};
            std::snprintf(ascii.data(), ascii.size(), "%04d-%02u-%02u", year,
                          static_cast<unsigned>(civil.month()), static_cast<unsigned>(civil.day()));
            dates_[slot(std::string_view(ascii.data(), 10))] = {ascii, day * day_ms, day * 86'400};
        }
    }

    Ingest parse(std::string_view value) const {
        if (value.size() == 20 && value[19] == 'Z') {
            const auto& date = dates_[slot(value)];
            if (std::memcmp(date.ascii.data(), value.data(), 10) == 0 &&
                value[10] == 'T' && value[13] == ':' && value[16] == ':') {
                const int hour = input::detail::decimal_pair(value.data() + 11);
                const int minute = input::detail::decimal_pair(value.data() + 14);
                const int second = input::detail::decimal_pair(value.data() + 17);
                if (static_cast<unsigned>(hour) <= 23 && static_cast<unsigned>(minute) <= 59 &&
                    static_cast<unsigned>(second) <= 59) {
                    return {date.midnight_ms, date.midnight_seconds + (hour * 60 + minute) * 60 + second};
                }
            }
        }
        const int64_t full = input::ingest_ms(value);
        if (full == input::invalid_timestamp) {
            throw std::runtime_error("Invalid ingest_ts on a parsed event");
        }
        if (full % 1000 != 0) {
            // The packed ticket only represents whole-second ingest times.
            return {0, input::invalid_timestamp};
        }
        const auto remainder = full % day_ms;
        return {full - (remainder < 0 ? remainder + day_ms : remainder), full / 1000};
    }
};

inline std::optional<Input> scan(const fs::path& data, int64_t start, int64_t end,
                                 const IdKeys& user_keys, const Products& products,
                                 unsigned threads) {
    Input batch{Partitioned<Ticket>(threads), Partitioned<Event>(threads)};
    IdKeys event_keys;
    const IngestDates dates(start, end);
    std::atomic<bool> need_wide{false};
    scan_shards<7>(data / "events", threads, [&](const auto& row, unsigned worker) {
        if (row[0].empty()) {
            return;
        }
        const auto ingest = dates.parse(row[3]);
        if (ingest.seconds == input::invalid_timestamp) {
            need_wide.store(true, std::memory_order_relaxed);
            return;
        }
        const auto ingest_rank = static_cast<uint64_t>(ingest.seconds + Ticket::ingest_bias);
        if (ingest_rank >= (uint64_t{1} << 39)) {
            need_wide.store(true, std::memory_order_relaxed);
            return;
        }

        uint32_t handle = 0;
        const auto event = event_payload<uint32_t>(
            row, [&](int64_t time) { return ingest.late(time); }, start, end, start, products,
            [&](std::string_view user) { return user_keys.key(user); });
        if (event.person != 0) {
            const size_t bucket = bucket_for_key(event.person);
            auto& rows = batch.payloads[worker][bucket];
            if (rows.size() == Ticket::highest_index) {
                need_wide.store(true, std::memory_order_relaxed);
                return;
            }
            rows.push_back(event);
            handle = static_cast<uint32_t>((rows.size() << 8) | bucket);
        }
        // Even an invalid earliest copy suppresses later valid copies.
        const uint64_t hash = hash_id(event_keys.key(row[0]));
        const size_t owner = (hash >> 32) % partition_count;
        batch.tickets[worker][owner].push_back(Ticket::encode(hash, ingest_rank, handle));
    }, [&](const auto& files) {
        reserve_copies(files, batch.tickets);
        reserve_copies(files, batch.payloads);
    });

    if (need_wide.load(std::memory_order_relaxed)) {
        return std::nullopt;
    }
    return batch;
}

inline void deduplicate(Input& input, unsigned threads) {
    auto discard = [&](unsigned worker, uint32_t handle) {
        if (handle != 0) {
            input.payloads[worker][handle & 255][(handle >> 8) - 1].person = 0;
        }
    };
    // Each ticket and its payload belong to one ID bucket. Different
    // dedup workers update distinct payload elements with stable addresses.
    parallel_for(partition_count, threads, [&](size_t bucket, unsigned) {
        size_t count = 0;
        for (const auto& producer : input.tickets) {
            count += producer[bucket].size();
        }
        if (count == 0) {
            return;
        }

        struct SignatureWord {
            uint64_t seen = 0;
            uint64_t ambiguous = 0;
        };
        std::vector<SignatureWord> signatures;
        size_t signature_mask = 0;
        size_t candidates = count;
        if (count >= 1024) {
            // The same exact key always goes to the same bit. A bit set by
            // just one ticket certifies that key as a singleton.
            const size_t domain = std::bit_ceil(std::min(count, size_t{1} << 20) * 8);
            signature_mask = domain - 1;
            signatures.resize(domain / 64);
            candidates = 0;
            for (const auto& source : input.tickets) {
                for (const auto& copy : source[bucket]) {
                    const size_t signature = copy.key() & signature_mask;
                    const uint64_t bit = uint64_t{1} << (signature % 64);
                    auto& word = signatures[signature / 64];
                    const uint64_t repeated = word.seen & bit;
                    // The second hit counts both tickets. Each further hit
                    // adds only its own ticket to the exact table size.
                    candidates += (repeated != 0) + ((repeated & ~word.ambiguous) != 0);
                    word.seen |= bit;
                    word.ambiguous |= repeated;
                }
                if (candidates > count / 3) {
                    std::vector<SignatureWord>().swap(signatures);
                    candidates = count;
                    break;
                }
            }
        }

        constexpr uintptr_t producer_bits = alignof(Ticket) - 1;
        auto ticket_of = [](uintptr_t link) { return reinterpret_cast<Ticket*>(link & ~producer_bits); };
        EventWinnerSlots winners(candidates);
        for (size_t worker = 0; worker < input.tickets.size(); ++worker) {
            for (auto& copy : input.tickets[worker][bucket]) {
                if (!signatures.empty()) {
                    const size_t signature = copy.key() & signature_mask;
                    const uint64_t bit = uint64_t{1} << (signature % 64);
                    if ((signatures[signature / 64].ambiguous & bit) == 0) {
                        continue;
                    }
                }
                auto& link = winners.find(copy.hash(bucket), [&](uintptr_t candidate) {
                    return ticket_of(candidate)->key() == copy.key();
                });
                auto* winner = ticket_of(link);
                if (winner == nullptr || copy.ingest() < winner->ingest()) {
                    if (winner != nullptr) {
                        discard(link & producer_bits, winner->handle());
                    }
                    link = reinterpret_cast<uintptr_t>(&copy) | worker;
                } else {
                    discard(worker, copy.handle());
                }
            }
        }
        for (auto& producer : input.tickets) {
            std::vector<Ticket>().swap(producer[bucket]);
        }
    });
}

template<class OnSessions>
EventTotals make_sessions(Input& input, const Identities& identities, int64_t start,
                          size_t category_count, unsigned threads, StageTimer& timer,
                          Partitioned<CompactClick>& raw_clicks, OnSessions&& on_sessions) {
    Partitioned<Event> forwarded(threads);
    Partitioned<CompactClick> forwarded_clicks(raw_clicks.empty() ? 0 : threads);
    std::array<std::unique_ptr<session_bins::Bins>, partition_count> resident;
    parallel_for(partition_count, threads, [&](size_t bucket, unsigned worker) {
        size_t count = 0;
        for (const auto& producer : input.payloads) {
            count += producer[bucket].size();
        }
        if (count == 0 && raw_clicks.empty()) {
            return;
        }

        auto bins = count != 0 ? std::make_unique<session_bins::Bins>(count) : nullptr;
        for (auto& producer : input.payloads) {
            auto& rows = producer[bucket];
            for (auto event : rows) {
                const uint64_t raw_user = event.person;
                if (raw_user == 0) {
                    continue;
                }
                const uint64_t person = identities.canonical(raw_user);
                const auto owner = person == raw_user ? bucket : bucket_for_key(person);
                if (owner == bucket) {
                    bins->add(person, event.timestamp, event.price(), event.category());
                } else {
                    event.person = person;
                    forwarded[worker][owner].push_back(event);
                }
            }
            std::vector<Event>().swap(rows);
        }
        for (auto& producer : raw_clicks) {
            auto& rows = producer[bucket];
            size_t retained = 0;
            for (auto click : rows) {
                const uint64_t raw_user = click.person;
                click.person = identities.canonical(raw_user);
                const auto owner = click.person == raw_user ? bucket : bucket_for_key(click.person);
                if (owner == bucket) {
                    // Keep clicks whose canonical person still belongs to
                    // this bucket in their existing buffer.
                    rows[retained++] = click;
                } else {
                    forwarded_clicks[worker][owner].push_back(click);
                }
            }
            rows.resize(retained);
        }
        resident[bucket] = std::move(bins);
    });
    raw_clicks.insert(raw_clicks.end(), std::make_move_iterator(forwarded_clicks.begin()),
                      std::make_move_iterator(forwarded_clicks.end()));
    timer.report("Resolve raw ID partitions and group events and clicks");

    EventTotals totals;
    parallel_for(partition_count, threads, [&](size_t bucket, unsigned worker) {
        auto& bins = resident[bucket];
        for (auto& source : forwarded) {
            auto& rows = source[bucket];
            if (!rows.empty() && !bins) {
                bins = std::make_unique<session_bins::Bins>(rows.size());
            }
            for (const auto& event : rows) {
                bins->add(event.person, event.timestamp, event.price(), event.category());
            }
            std::vector<Event>().swap(rows);
        }
        if (bins) {
            session_bins::summarize(*bins, start, category_count,
                                   totals.sessions[bucket], totals.days[bucket]);
            bins.reset();
        }
        on_sessions(bucket, worker, totals.sessions[bucket]);
    });
    return totals;
}

}  // namespace packed_events
}  // namespace shopping
