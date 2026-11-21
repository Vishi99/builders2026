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

// Clicks are already bucketed by canonical person. campaign_count excludes organic.
template<class Time, class ClickKeys>
std::vector<CampaignDay> attribute_partitioned(
    int64_t start_ms, int64_t end_ms, const std::vector<std::vector<Session>>& sessions,
    Partitioned<attribution::ClickRecord<Time>> worker_clicks, const ClickKeys& click_keys,
    size_t campaign_count, unsigned threads, int64_t time_base
) {
    using namespace attribution;
    using Click = ClickRecord<Time>;
    const auto bucket_count = sessions.size();
    const auto first_day = day_number(start_ms);
    const size_t days = static_cast<int64_t>(day_number(end_ms - 1)) - first_day + 1;
    const auto campaigns = campaign_count + 1;
    // Preserve sparse storage for large date/dimension ranges.
    constexpr size_t dense_budget = 16 * 1024 * 1024 / sizeof(CampaignDay);
    const auto dense_days = days <= dense_budget / threads / campaigns ? days : 0;
    std::vector<Totals> worker_totals;
    worker_totals.reserve(threads);
    for (unsigned worker = 0; worker < threads; ++worker) {
        worker_totals.emplace_back(first_day, dense_days, campaigns);
    }
    std::vector worker_intervals(threads, std::vector<AssistInterval>(campaigns));
    std::vector<std::vector<uint32_t>> worker_touched(threads);
    parallel_for(bucket_count, threads, [&](size_t bucket, unsigned worker) {
        const auto& bucket_sessions = sessions[bucket];
        if (bucket_sessions.empty()) {
            return;
        }
        size_t click_count = 0;
        size_t largest = 0;
        for (size_t index = 0; index < worker_clicks.size(); ++index) {
            click_count += worker_clicks[index][bucket].size();
            if (worker_clicks[index][bucket].size() > worker_clicks[largest][bucket].size()) {
                largest = index;
            }
        }
        auto bucket_clicks = worker_clicks.empty() ? std::vector<Click>{}
                                                 : std::move(worker_clicks[largest][bucket]);
        bucket_clicks.reserve(click_count);
        for (size_t index = 0; index < worker_clicks.size(); ++index) {
            if (index == largest) {
                continue;
            }
            auto& clicks = worker_clicks[index][bucket];
            bucket_clicks.insert(bucket_clicks.end(), clicks.begin(), clicks.end());
            std::vector<Click>().swap(clicks);
        }
        std::sort(bucket_clicks.begin(), bucket_clicks.end(), [](const Click& left, const Click& right) {
            return left.person != right.person ? left.person < right.person
                                               : left.timestamp < right.timestamp;
        });

        auto& totals = worker_totals[worker];
        auto next_click = bucket_clicks.cbegin();
        for (auto session = bucket_sessions.cbegin(); session != bucket_sessions.cend();) {
            const auto person = session->person;
            auto session_end = session + 1;
            while (session_end != bucket_sessions.cend() && session_end->person == person) {
                ++session_end;
            }
            while (next_click != bucket_clicks.cend() && next_click->person < person) {
                ++next_click;
            }
            auto click_end = next_click;
            while (click_end != bucket_clicks.cend() && click_end->person == person) {
                ++click_end;
            }
            credit_sessions(totals, std::to_address(session), std::to_address(session_end),
                            std::to_address(next_click), std::to_address(click_end), click_keys, time_base);
            if (next_click != click_end) {
                assist_sessions(totals, std::to_address(session), std::to_address(session_end),
                                std::to_address(next_click), std::to_address(click_end),
                                worker_intervals[worker], worker_touched[worker], time_base);
            }
            session = session_end;
            next_click = click_end;
        }
    });

    Totals merged(first_day, dense_days, campaigns);
    for (const auto& totals : worker_totals) {
        totals.for_each([&](const CampaignDay& daily) {
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
