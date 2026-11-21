-- Check the exact syntax before parsing. Removing leading zeros lets a
-- padded small number fit in BIGINT. NULL from a failed cast or parse is rejected.
CREATE TEMP MACRO unsigned_int(value) AS (
    CASE WHEN regexp_full_match(value, '[0-9]+')
         THEN try_cast(coalesce(nullif(ltrim(value, '0'), ''), '0') AS BIGINT)
    END
);

CREATE TEMP MACRO week_ms() AS 604800000;

CREATE TEMP MACRO timestamp_ms(value) AS (
    CASE WHEN regexp_full_match(
        value,
        '[0-9]{4}-[0-9]{2}-[0-9]{2}T([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9](Z|[+-]([01][0-9]|2[0-3]):[0-5][0-9])'
    )
    THEN epoch_ms(try_strptime(left(value, 19), '%Y-%m-%dT%H:%M:%S'))
         - CASE
               WHEN right(value, 1) = 'Z' THEN 0
               ELSE (CASE WHEN substr(value, 20, 1) = '-' THEN -1 ELSE 1 END)
                    * (substr(value, 21, 2)::BIGINT * 60 + substr(value, 24, 2)::BIGINT)
                    * 60000
           END
    ELSE unsigned_int(value)
    END
);

-- Count with a narrow hash table first. Most IDs occur once and can pass
-- through without storing a full event row in an aggregate.
CREATE TEMP TABLE duplicate_event_ids AS
SELECT event_id
FROM raw_events
GROUP BY event_id
HAVING count(*) > 1;

-- For repeated IDs, choose the entire earliest copy before validating any
-- of its fields. A struct preserves null fields in the winning copy.
CREATE TEMP TABLE dedup_events AS
SELECT user_id, ts, ingest_ts, event_type, product_id, price_cents
FROM raw_events
ANTI JOIN duplicate_event_ids USING (event_id)
UNION ALL
SELECT unnest(arg_min(
           struct_pack(user_id := user_id, ts := ts, ingest_ts := ingest_ts,
                       event_type := event_type, product_id := product_id,
                       price_cents := price_cents),
           ingest_ts
       ))
FROM raw_events
SEMI JOIN duplicate_event_ids USING (event_id)
GROUP BY event_id;
DROP TABLE duplicate_event_ids;
DROP TABLE raw_events;

CREATE TEMP TABLE clean_events AS
WITH parsed AS (
    SELECT user_id, product_id, event_type, timestamp_ms(ts) AS ts_ms,
           epoch_ms(strptime(ingest_ts, '%Y-%m-%dT%H:%M:%SZ')) AS ingest_ms,
           CASE WHEN event_type = 'purchase' THEN unsigned_int(price_cents) END AS price
    FROM dedup_events
    WHERE user_id IS NOT NULL AND product_id IS NOT NULL
      AND event_type IN ('view', 'cart', 'purchase')
)
SELECT coalesce(person_ids.person, parsed.user_id) AS person, ts_ms,
       product_id, price
FROM parsed
CROSS JOIN dataset_window
LEFT JOIN person_ids ON parsed.user_id = person_ids.user_id
WHERE ts_ms >= start_ms AND ts_ms < end_ms
  AND ts_ms >= ingest_ms - 86400000
  AND (event_type <> 'purchase' OR price BETWEEN 1 AND 10000000);
DROP TABLE dedup_events;

CREATE TEMP TABLE session_events AS
WITH event_gaps AS (
    SELECT *, lag(ts_ms) OVER (PARTITION BY person ORDER BY ts_ms) AS previous_ms
    FROM clean_events
)
SELECT person, product_id, price,
       -- A RANGE frame includes all events at the same timestamp, so tied
       -- events get the same start even if only one of them follows a gap.
       max(CASE WHEN previous_ms IS NULL OR ts_ms - previous_ms > 1800000
                THEN ts_ms END) OVER (
           PARTITION BY person ORDER BY ts_ms
           RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
       ) AS session_start
FROM event_gaps;
DROP TABLE clean_events;

CREATE TEMP TABLE sessions AS
SELECT person, session_start, make_timestamp_ms(session_start)::DATE AS day,
       count(*) AS events, coalesce(sum(price), 0)::BIGINT AS revenue_cents,
       count(price) > 0 AS purchased
FROM session_events
GROUP BY person, session_start;

-- Category rank uses all purchases assigned to the person's session-start day.
CREATE TEMP TABLE user_daily AS
WITH day_totals AS (
    SELECT person, day, count(*) AS sessions, sum(events)::BIGINT AS events,
           sum(revenue_cents)::BIGINT AS revenue_cents
    FROM sessions
    GROUP BY person, day
), category_totals AS (
    SELECT person, make_timestamp_ms(session_start)::DATE AS day,
           coalesce(category, 'unknown') AS category, sum(price) AS revenue
    FROM session_events
    LEFT JOIN products USING (product_id)
    WHERE price IS NOT NULL
    GROUP BY person, day, coalesce(category, 'unknown')
), winners AS (
    SELECT person, day, category
    FROM category_totals
    QUALIFY row_number() OVER (
        PARTITION BY person, day ORDER BY revenue DESC, encode(category)
    ) = 1
)
SELECT person AS user_id, day, sessions, events, revenue_cents, category AS top_category
FROM day_totals
LEFT JOIN winners USING (person, day);

DROP TABLE session_events;
DROP TABLE products;
