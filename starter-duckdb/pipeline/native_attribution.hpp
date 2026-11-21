#pragma once

#include "native_events.hpp"

#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <memory>
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

struct Click {
    uint64_t person;
    int64_t timestamp;
    uint64_t id;
    uint32_t campaign;
};

struct AssistInterval {
    int64_t first = 0;
    int64_t last = 0;
    bool active = false;
};

using Totals = std::unordered_map<uint64_t, CampaignDay>;

inline CampaignDay& campaign_day(Totals& totals, uint32_t campaign, int32_t day) {
    const auto key = (uint64_t{campaign} << 32) | static_cast<uint32_t>(day);
    return totals.try_emplace(key, CampaignDay{campaign, day}).first->second;
}

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
        campaign_day(totals, campaign, day).assisted_sessions += next - session;
        session = next;
    }
}

template<class ClickKeys>
void credit_sessions(Totals& totals, const Session* sessions, const Session* session_end,
                     const Click* clicks, const Click* click_end, const ClickKeys& click_keys) {
    const Click* winner = nullptr;
    for (auto* session = sessions; session != session_end; ++session) {
        while (clicks != click_end && clicks->timestamp <= session->start_ms) {
            if (!winner || clicks->timestamp != winner->timestamp ||
                click_keys.less(clicks->id, winner->id)) {
                winner = clicks;
            }
            ++clicks;
        }
        const auto campaign = winner && session->start_ms - winner->timestamp <= week_ms
            ? winner->campaign : 0;
        auto& daily = campaign_day(totals, campaign, day_number(session->start_ms));
        ++daily.sessions;
        daily.purchasing_sessions += session->revenue != 0;
        daily.revenue_cents += session->revenue;
    }
}

inline void assist_sessions(Totals& totals, const Session* sessions, const Session* session_end,
                            const Click* clicks, const Click* click_end,
                            std::vector<AssistInterval>& intervals, std::vector<uint32_t>& touched) {
    // Restrict the sorted clicks to those that can reach any of this
    // person's sessions before building the per-campaign interval union.
    const auto earliest = sessions->start_ms - week_ms;
    const auto latest = session_end[-1].start_ms;
    while (clicks != click_end && clicks->timestamp < earliest) {
        ++clicks;
    }
    for (; clicks != click_end && clicks->timestamp <= latest; ++clicks) {
        auto& interval = intervals[clicks->campaign];
        if (!interval.active) {
            touched.push_back(clicks->campaign);
            interval.active = true;
            interval.first = clicks->timestamp;
        } else if (clicks->timestamp - interval.last > week_ms) {
            count_assists(totals, clicks->campaign, interval, sessions, session_end);
            interval.first = clicks->timestamp;
        }
        interval.last = clicks->timestamp;
    }
    for (const auto campaign : touched) {
        auto& interval = intervals[campaign];
        count_assists(totals, campaign, interval, sessions, session_end);
        interval.active = false;
    }
    touched.clear();
}

} // namespace attribution

template<class Ids, class ClickKeys, class CampaignIds>
std::vector<CampaignDay> attribute(
    const std::filesystem::path& data, int64_t start_ms, int64_t end_ms,
    const std::vector<std::vector<Session>>& sessions, const Ids& identities, ClickKeys& click_keys,
    const CampaignIds& campaign_ids, unsigned threads
) {
    using namespace attribution;
    const auto bucket_count = sessions.size();
    std::vector worker_clicks(threads, std::vector<std::vector<Click>>(bucket_count));

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
        const auto person = identities.person(fields[1]);
        const auto bucket = bucket_for_key(person);
        if (!sessions[bucket].empty()) {
            // Identical click copies are harmless to both the minimum-ID
            // tiebreak and the union of each campaign's assist intervals.
            worker_clicks[worker][bucket].push_back(
                Click{person, timestamp, click_keys.key(fields[0]), campaign->second});
        }
    });

    std::vector<Totals> worker_totals(threads);
    std::vector worker_intervals(threads, std::vector<AssistInterval>(campaign_ids.size() + 1));
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
        auto bucket_clicks = std::move(worker_clicks[largest][bucket]);
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
                            std::to_address(next_click), std::to_address(click_end), click_keys);
            if (next_click != click_end) {
                assist_sessions(totals, std::to_address(session), std::to_address(session_end),
                                std::to_address(next_click), std::to_address(click_end),
                                worker_intervals[worker], worker_touched[worker]);
            }
            session = session_end;
            next_click = click_end;
        }
    });

    Totals merged;
    for (const auto& totals : worker_totals) {
        for (const auto& entry : totals) {
            const auto& daily = entry.second;
            auto& combined = campaign_day(merged, daily.campaign, daily.day);
            combined.sessions += daily.sessions;
            combined.purchasing_sessions += daily.purchasing_sessions;
            combined.revenue_cents += daily.revenue_cents;
            combined.assisted_sessions += daily.assisted_sessions;
        }
    }
    std::vector<CampaignDay> result;
    result.reserve(merged.size());
    for (const auto& entry : merged) {
        result.push_back(entry.second);
    }
    return result;
}

} // namespace shopping
