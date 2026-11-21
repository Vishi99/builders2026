#pragma once

#include "native_events.hpp"

#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

namespace shopping {

struct CampaignDay {
    uint32_t campaign;
    int32_t day;
    int64_t sessions = 0;
    int64_t purchasing_sessions = 0;
    int64_t revenue_cents = 0;
    int64_t assisted_sessions = 0;
};

namespace attribution {

template<class Time>
struct ClickRecord {
    uint64_t person;
    uint64_t id;
    Time timestamp;
    uint32_t campaign;
};

struct AssistInterval {
    int64_t first = 0;
    int64_t last = 0;
    bool active = false;
};

class Totals {
public:
    Totals(int32_t first, size_t days, size_t campaigns)
        : first_(first), days_(days), dense_(days * campaigns) {}

    CampaignDay& at(uint32_t campaign, int32_t day) {
        if (!dense_.empty()) {
            return dense_[campaign * days_ + static_cast<size_t>(day - first_)];
        }
        const auto key = (uint64_t{campaign} << 32) | static_cast<uint32_t>(day);
        return sparse_.try_emplace(key, CampaignDay{campaign, day}).first->second;
    }

    template<class Visit>
    void for_each(Visit visit) const {
        if (dense_.empty()) {
            for (const auto& [key, value] : sparse_) {
                visit(value);
            }
            return;
        }
        for (size_t index = 0; index < dense_.size(); ++index) {
            auto value = dense_[index];
            if (value.sessions || value.assisted_sessions) {
                value.campaign = static_cast<uint32_t>(index / days_);
                value.day = first_ + static_cast<int32_t>(index % days_);
                visit(value);
            }
        }
    }

private:
    int32_t first_;
    size_t days_;
    std::vector<CampaignDay> dense_;
    std::unordered_map<uint64_t, CampaignDay> sparse_;
};

inline const Session* session_at_or_after(const Session* begin, const Session* end,
                                          int64_t timestamp) {
    return std::lower_bound(begin, end, timestamp,
                            [](const Session& session, int64_t time) {
                                return session.start_ms < time;
                            });
}

inline void count_assists(Totals& totals, uint32_t campaign, const AssistInterval& interval,
                          const Session* sessions, const Session* end) {
    const auto* session = session_at_or_after(sessions, end, interval.first);
    const auto last = interval.last + week_ms;
    while (session != end && session->start_ms <= last) {
        const int32_t day = day_number(session->start_ms);
        const int64_t next_day = (static_cast<int64_t>(day) + 1) * day_ms;
        // Session times are integer milliseconds, so last + 1 excludes the
        // first session outside the inclusive seven-day interval.
        const auto* next = session_at_or_after(session + 1, end, std::min(next_day, last + 1));
        totals.at(campaign, day).assisted_sessions += next - session;
        session = next;
    }
}

template<class Click, class ClickKeys>
void credit_sessions(Totals& totals, const Session* sessions, const Session* session_end,
                     const Click* clicks, const Click* click_end, const ClickKeys& click_keys,
                     int64_t time_base) {
    const Click* winner = nullptr;
    for (auto* session = sessions; session != session_end; ++session) {
        while (clicks != click_end && clicks->timestamp <= session->start_ms - time_base) {
            if (!winner || clicks->timestamp != winner->timestamp ||
                click_keys.less(clicks->id, winner->id)) {
                winner = clicks;
            }
            ++clicks;
        }
        const auto campaign = winner && session->start_ms - time_base - winner->timestamp <= week_ms
            ? winner->campaign : 0;
        auto& daily = totals.at(campaign, day_number(session->start_ms));
        ++daily.sessions;
        daily.purchasing_sessions += session->revenue != 0;
        daily.revenue_cents += session->revenue;
    }
}

template<class Click>
void assist_sessions(Totals& totals, const Session* sessions, const Session* session_end,
                            const Click* clicks, const Click* click_end,
                            std::vector<AssistInterval>& intervals, std::vector<uint32_t>& touched,
                            int64_t time_base) {
    // Restrict the sorted clicks to those that can reach any of this
    // person's sessions before building the per-campaign interval union.
    const auto earliest = sessions->start_ms - week_ms - time_base;
    const auto latest = session_end[-1].start_ms - time_base;
    while (clicks != click_end && clicks->timestamp < earliest) {
        ++clicks;
    }
    for (; clicks != click_end && clicks->timestamp <= latest; ++clicks) {
        auto& interval = intervals[clicks->campaign];
        if (!interval.active) {
            touched.push_back(clicks->campaign);
            interval.active = true;
            interval.first = time_base + clicks->timestamp;
        } else if (time_base + clicks->timestamp - interval.last > week_ms) {
            count_assists(totals, clicks->campaign, interval, sessions, session_end);
            interval.first = time_base + clicks->timestamp;
        }
        interval.last = time_base + clicks->timestamp;
    }
    for (const auto campaign : touched) {
        auto& interval = intervals[campaign];
        count_assists(totals, campaign, interval, sessions, session_end);
        interval.active = false;
    }
    touched.clear();
}

} // namespace attribution

template<class Time, class ClickKeys, class CampaignIds, class ResolveUser>
Partitioned<attribution::ClickRecord<Time>> scan_click_records(
    const std::filesystem::path& data, int64_t start_ms, int64_t end_ms,
    ClickKeys& click_keys, const CampaignIds& campaign_ids, unsigned threads, int64_t time_base,
    ResolveUser&& resolve_user, const std::vector<std::vector<Session>>* sessions = nullptr
) {
    using Click = attribution::ClickRecord<Time>;
    Partitioned<Click> worker_clicks(threads);

    scan_shards<4>(data / "clicks", threads, [&](const auto& fields, unsigned worker) {
        if (fields[0].empty() || fields[1].empty()) {
            return;
        }
        const auto campaign = campaign_ids.find(fields[2]);
        if (campaign == campaign_ids.end()) {
            return;
        }
        const auto timestamp = input::timestamp_ms(fields[3]);
        if (timestamp == input::invalid_timestamp ||
            timestamp < start_ms - week_ms || timestamp >= end_ms) {
            return;
        }
        const auto person = resolve_user(fields[1], worker);
        const auto bucket = bucket_for_key(person);
        if (sessions == nullptr || !(*sessions)[bucket].empty()) {
            // Identical click copies are harmless to both the minimum-ID
            // tiebreak and the union of each campaign's assist intervals.
            worker_clicks[worker][bucket].push_back(
                Click{person, click_keys.key(fields[0]), static_cast<Time>(timestamp - time_base), campaign->second});
        }
    }, [&](const auto& files) {
        reserve_copies(files, worker_clicks);
    });
    return worker_clicks;
}

namespace attribution {

struct CampaignWorker {
    Totals totals;
    std::vector<AssistInterval> intervals;
    std::vector<uint32_t> touched;
};

class CampaignAccumulator {
public:
    CampaignAccumulator(int64_t start_ms, int64_t end_ms, size_t campaign_count,
                        unsigned threads)
        : first_day_(day_number(start_ms)), campaigns_(campaign_count + 1) {
        const size_t days = static_cast<int64_t>(day_number(end_ms - 1)) - first_day_ + 1;
        // Preserve sparse storage for large date/dimension ranges.
        constexpr size_t dense_budget = 16 * 1024 * 1024 / sizeof(CampaignDay);
        dense_days_ = days <= dense_budget / threads / campaigns_ ? days : 0;
        workers_.reserve(threads);
        for (unsigned worker = 0; worker < threads; ++worker) {
            workers_.push_back({Totals(first_day_, dense_days_, campaigns_),
                                std::vector<AssistInterval>(campaigns_), {}});
        }
    }

    CampaignWorker& worker(unsigned index) {
        return workers_[index];
    }

    std::vector<CampaignDay> finish() const {
        Totals merged(first_day_, dense_days_, campaigns_);
        for (const auto& worker : workers_) {
            worker.totals.for_each([&](const CampaignDay& daily) {
                auto& combined = merged.at(daily.campaign, daily.day);
                combined.sessions += daily.sessions;
                combined.purchasing_sessions += daily.purchasing_sessions;
                combined.revenue_cents += daily.revenue_cents;
                combined.assisted_sessions += daily.assisted_sessions;
            });
        }
        std::vector<CampaignDay> result;
        merged.for_each([&](const CampaignDay& daily) {
            result.push_back(daily);
        });
        return result;
    }

private:
    int32_t first_day_;
    size_t campaigns_;
    size_t dense_days_;
    std::vector<CampaignWorker> workers_;
};

template<class Time, class ClickKeys>
void attribute_owner(const std::vector<Session>& bucket_sessions,
                     Partitioned<ClickRecord<Time>>& worker_clicks, size_t bucket,
                     CampaignWorker& worker, const ClickKeys& click_keys, int64_t time_base) {
    using Click = ClickRecord<Time>;
    if (bucket_sessions.empty()) {
        for (auto& producer : worker_clicks) {
            std::vector<Click>().swap(producer[bucket]);
        }
        return;
    }
    if (std::none_of(worker_clicks.begin(), worker_clicks.end(), [bucket](const auto& producer) {
            return !producer[bucket].empty();
        })) {
        // An empty click partition credits all sessions to organic.
        const Click* empty = nullptr;
        credit_sessions(worker.totals, bucket_sessions.data(),
                        bucket_sessions.data() + bucket_sessions.size(),
                        empty, empty, click_keys, time_base);
        return;
    }
    struct PersonGroup {
        uint64_t person;
        size_t session_end;
        size_t click_count = 0;
        size_t next_click = 0;
    };
    struct GroupClick {
        uint64_t id;
        Time timestamp;
        uint32_t campaign;
    };

    // Sessions already group by person. Use their groups as exact dense IDs
    // so we only sort a person's own clicks, comparing timestamps alone.
    std::vector<PersonGroup> groups;
    groups.reserve(bucket_sessions.size() / 5);
    for (size_t start = 0; start < bucket_sessions.size();) {
        const auto person = bucket_sessions[start].person;
        size_t end = start + 1;
        while (end < bucket_sessions.size() && bucket_sessions[end].person == person) {
            ++end;
        }
        groups.push_back({person, end});
        start = end;
    }
    IdMap index;
    index.reserve(groups.size());
    for (size_t group = 0; group < groups.size(); ++group) {
        index.get_or_insert(groups[group].person, [&] { return group + 1; });
    }
    // Count each group's clicks and replace the person key with its group
    // number. Clicks with no session group cannot credit or assist.
    for (auto& producer : worker_clicks) {
        for (auto& click : producer[bucket]) {
            click.person = index.get(click.person, hash_id(click.person));
            if (click.person != 0) {
                ++groups[click.person - 1].click_count;
            }
        }
    }
    size_t total_clicks = 0;
    for (auto& group : groups) {
        group.next_click = total_clicks;
        total_clicks += group.click_count;
    }
    // The prefix offsets give each group a contiguous time list. The
    // compact records no longer need to carry a person key.
    auto grouped = std::make_unique_for_overwrite<GroupClick[]>(total_clicks);
    for (auto& producer : worker_clicks) {
        auto& clicks = producer[bucket];
        for (const auto& click : clicks) {
            if (click.person != 0) {
                auto& group = groups[click.person - 1];
                grouped[group.next_click++] = {click.id, click.timestamp, click.campaign};
            }
        }
        std::vector<Click>().swap(clicks);
    }
    const auto* sessions = bucket_sessions.data();
    for (const auto& group : groups) {
        const auto* session_end = bucket_sessions.data() + group.session_end;
        auto* end = grouped.get() + group.next_click;
        auto* begin = end - group.click_count;
        if (group.click_count > 1) {
            std::sort(begin, end, [](const GroupClick& a, const GroupClick& b) {
                return a.timestamp < b.timestamp;
            });
        }
        credit_sessions(worker.totals, sessions, session_end, begin, end, click_keys, time_base);
        if (begin != end) {
            assist_sessions(worker.totals, sessions, session_end, begin, end,
                            worker.intervals, worker.touched, time_base);
        }
        sessions = session_end;
    }
}

}  // namespace attribution

// Clicks are already bucketed by canonical person. campaign_count excludes organic.
template<class Time, class ClickKeys>
std::vector<CampaignDay> attribute_partitioned(
    int64_t start_ms, int64_t end_ms, const std::vector<std::vector<Session>>& sessions,
    Partitioned<attribution::ClickRecord<Time>> worker_clicks, const ClickKeys& click_keys,
    size_t campaign_count, unsigned threads, int64_t time_base
) {
    attribution::CampaignAccumulator accumulator(start_ms, end_ms, campaign_count, threads);
    parallel_for(sessions.size(), threads, [&](size_t bucket, unsigned worker) {
        attribution::attribute_owner(sessions[bucket], worker_clicks, bucket,
                                      accumulator.worker(worker), click_keys, time_base);
    });
    return accumulator.finish();
}

template<class Time, class Ids, class ClickKeys, class CampaignIds>
std::vector<CampaignDay> attribute_records(
    const std::filesystem::path& data, int64_t start_ms, int64_t end_ms,
    const std::vector<std::vector<Session>>& sessions, const Ids& identities, ClickKeys& click_keys,
    const CampaignIds& campaign_ids, unsigned threads, int64_t time_base
) {
    std::vector<typename Ids::LookupCache> people(threads);
    auto clicks = scan_click_records<Time>(
        data, start_ms, end_ms, click_keys, campaign_ids, threads, time_base,
        [&](std::string_view user, unsigned worker) {
            return identities.person(user, people[worker]);
        }, &sessions);
    return attribute_partitioned(start_ms, end_ms, sessions, std::move(clicks), click_keys,
                                 campaign_ids.size(), threads, time_base);
}

template<class Ids, class ClickKeys, class CampaignIds>
std::vector<CampaignDay> attribute(
    const std::filesystem::path& data, int64_t start_ms, int64_t end_ms,
    const std::vector<std::vector<Session>>& sessions, const Ids& identities, ClickKeys& click_keys,
    const CampaignIds& campaign_ids, unsigned threads
) {
    if (std::all_of(sessions.begin(), sessions.end(), [](const auto& bucket) {
            return bucket.empty();
        })) {
        return {};
    }

    const auto time_base = start_ms - week_ms;
    const auto span = static_cast<uint64_t>(end_ms) - static_cast<uint64_t>(time_base);
    if (span <= (uint64_t{1} << 32)) {
        return attribute_records<uint32_t>(data, start_ms, end_ms, sessions, identities,
                                           click_keys, campaign_ids, threads, time_base);
    }
    return attribute_records<int64_t>(data, start_ms, end_ms, sessions, identities,
                                      click_keys, campaign_ids, threads, 0);
}

} // namespace shopping
