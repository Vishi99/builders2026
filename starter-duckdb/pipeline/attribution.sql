CREATE TEMP TABLE clicks AS
WITH dedup AS (
    -- Parsed copies of a click have identical values.
    SELECT DISTINCT ON (click_id) * FROM raw_clicks
), parsed AS (
    SELECT click_id, user_id, campaign_id, timestamp_ms(ts) AS ts_ms
    FROM dedup
    WHERE user_id IS NOT NULL
)
SELECT coalesce(person_ids.person, parsed.user_id) AS person,
       click_id, parsed.campaign_id, ts_ms
FROM parsed
JOIN campaigns USING (campaign_id)
CROSS JOIN dataset_window
LEFT JOIN person_ids ON parsed.user_id = person_ids.user_id
WHERE ts_ms >= start_ms - week_ms() AND ts_ms < end_ms;
DROP TABLE raw_clicks;
DROP TABLE person_ids;

CREATE TEMP TABLE credited_daily AS
WITH timestamp_winners AS (
    SELECT person, ts_ms, arg_min(campaign_id, encode(click_id)) AS campaign_id
    FROM clicks
    GROUP BY person, ts_ms
), credited AS (
    -- Latest click at or before the start. A missing or stale match is organic.
    SELECT sessions.day, sessions.purchased, sessions.revenue_cents,
           CASE WHEN sessions.session_start - timestamp_winners.ts_ms <= week_ms()
                THEN timestamp_winners.campaign_id ELSE 'organic' END AS campaign_id
    FROM sessions
    ASOF LEFT JOIN timestamp_winners
      ON sessions.person = timestamp_winners.person
     AND sessions.session_start >= timestamp_winners.ts_ms
)
SELECT campaign_id, day, count(*) AS sessions,
       count(*) FILTER (WHERE purchased) AS purchasing_sessions,
       sum(revenue_cents)::BIGINT AS revenue_cents
FROM credited
GROUP BY campaign_id, day;

-- A click at t assists starts in [t, t + 7 days]. Merge overlapping
-- intervals per person and campaign so repeated clicks count only once per
-- session. Include simultaneous clicks to different campaigns.
CREATE TEMP TABLE assisted_daily AS
WITH click_times AS (
    SELECT DISTINCT person, campaign_id, ts_ms FROM clicks
), gaps AS (
    SELECT *, lag(ts_ms) OVER (
        PARTITION BY person, campaign_id ORDER BY ts_ms
    ) AS previous_ms
    FROM click_times
), interval_groups AS (
    SELECT *, count(*) FILTER (
        WHERE previous_ms IS NULL OR ts_ms - previous_ms > week_ms()
    ) OVER (
        PARTITION BY person, campaign_id ORDER BY ts_ms
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS interval_id
    FROM gaps
), intervals AS (
    SELECT person, campaign_id, min(ts_ms) AS first_start,
           max(ts_ms) + week_ms() AS last_start
    FROM interval_groups
    GROUP BY person, campaign_id, interval_id
)
SELECT intervals.campaign_id, sessions.day, count(*) AS assisted_sessions
FROM intervals
JOIN sessions ON sessions.person = intervals.person
             AND sessions.session_start BETWEEN intervals.first_start AND intervals.last_start
GROUP BY intervals.campaign_id, sessions.day;

-- A campaign may have assists on a day without receiving any credit.
CREATE TEMP TABLE campaign_daily AS
WITH combined AS (
    SELECT campaign_id, day, coalesce(sessions, 0) AS sessions,
           coalesce(purchasing_sessions, 0) AS purchasing_sessions,
           coalesce(revenue_cents, 0) AS revenue_cents,
           coalesce(assisted_sessions, 0) AS assisted_sessions
    FROM credited_daily
    FULL OUTER JOIN assisted_daily USING (campaign_id, day)
)
SELECT campaign_id, CASE WHEN campaign_id = 'organic' THEN 'organic'
                         ELSE channel END AS channel,
       day, sessions, purchasing_sessions, revenue_cents, assisted_sessions
FROM combined
LEFT JOIN campaigns USING (campaign_id);

DROP TABLE clicks;
DROP TABLE sessions;
DROP TABLE credited_daily;
DROP TABLE assisted_daily;
