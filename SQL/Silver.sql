--SILVER LAYER: 

USE DATABASE GOPHER_DB;
CREATE SCHEMA IF NOT EXISTS DEMO_SILVER;
USE SCHEMA DEMO_SILVER;


-- Create Tables

CREATE OR REPLACE TABLE STREAM_SESSION (
  event_id            STRING PRIMARY KEY,
  occurred_at         TIMESTAMP_TZ,
  occurred_date       DATE,
  occurred_time       TIME,
  occurred_hour       NUMBER,
  received_at         TIMESTAMP_TZ,
  received_date       DATE,
  received_time       TIME,
  received_hour       NUMBER,
  user_id             STRING,
  household_id        STRING,
  profile_id          STRING,
  session_id          STRING,
  device_id           STRING,
  location_id         STRING,
  content_id          STRING,
  country             STRING,
  experiment_id       STRING,
  variant             STRING,
  start_ts            TIMESTAMP_TZ,
  end_ts              TIMESTAMP_TZ,
  duration_sec        NUMBER,
  playhead_sec        NUMBER,
  avg_bitrate_kbps    NUMBER,
  rebuffer_count      NUMBER,
  rebuffer_sec        NUMBER,
  join_time_ms        NUMBER,
  network_type        STRING,
  device_model        STRING
);

CREATE OR REPLACE TABLE ENGAGEMENT_EVENT (
  event_id       STRING PRIMARY KEY,
  occurred_at    TIMESTAMP_TZ,
  occurred_date  DATE,
  occurred_time  TIME,
  occurred_hour  NUMBER,
  received_at    TIMESTAMP_TZ,
  received_date  DATE,
  received_time  TIME,
  received_hour  NUMBER,
  user_id        STRING,
  household_id   STRING,
  profile_id     STRING,
  session_id     STRING,
  device_id      STRING,
  location_id    STRING,
  content_id     STRING,
  action         STRING,
  funnel_stage   STRING,
  playhead_sec   NUMBER,
  extra          VARIANT,
  play_source    STRING
);

CREATE OR REPLACE TABLE SEARCH_EVENT (
  event_id              STRING PRIMARY KEY,
  occurred_at           TIMESTAMP_TZ,
  occurred_date         DATE,
  occurred_time         TIME,
  occurred_hour         NUMBER,
  received_at           TIMESTAMP_TZ,
  received_date         DATE,
  received_time         TIME,
  received_hour         NUMBER,
  user_id               STRING,
  household_id          STRING,
  profile_id            STRING,
  session_id            STRING,
  device_id             STRING,
  location_id           STRING,
  query                 STRING,
  results_returned      NUMBER,
  clicked_content_id    STRING,
  latency_ms            NUMBER
);

CREATE OR REPLACE TABLE BROWSE_EVENT (
  event_id          STRING PRIMARY KEY,
  occurred_at       TIMESTAMP_TZ,
  occurred_date     DATE,
  occurred_time     TIME,
  occurred_hour     NUMBER,
  received_at       TIMESTAMP_TZ,
  received_date     DATE,
  received_time     TIME,
  received_hour     NUMBER,
  user_id           STRING,
  household_id      STRING,
  profile_id        STRING,
  session_id        STRING,
  device_id         STRING,
  location_id       STRING,
  surface           STRING,
  view_type         STRING,
  nav_action        STRING,
  rail_id           STRING,
  position          NUMBER,
  impression_value  NUMBER
);

CREATE OR REPLACE TABLE TILE_IMPRESSION (
  event_id                STRING PRIMARY KEY,
  occurred_at             TIMESTAMP_TZ,
  occurred_date           DATE,
  occurred_time           TIME,
  occurred_hour           NUMBER,
  received_at             TIMESTAMP_TZ,
  received_date           DATE,
  received_time           TIME,
  received_hour           NUMBER,
  user_id                 STRING,
  household_id            STRING,
  profile_id              STRING,
  session_id              STRING,
  device_id               STRING,
  location_id             STRING,
  content_id              STRING,
  tile_id                 STRING,
  row_id                  STRING,
  position                NUMBER,
  dwell_ms                NUMBER,
  preview_watch_ms        NUMBER,
  clicked                 BOOLEAN,
  conversion_flag         BOOLEAN,
  conversion_latency_sec  NUMBER
);

CREATE OR REPLACE TABLE COMMERCE_EVENT (
  event_id        STRING PRIMARY KEY,
  occurred_at     TIMESTAMP_TZ,
  occurred_date   DATE,
  occurred_time   TIME,
  occurred_hour   NUMBER,
  received_at     TIMESTAMP_TZ,
  received_date   DATE,
  received_time   TIME,
  received_hour   NUMBER,
  user_id         STRING,
  household_id    STRING,
  profile_id      STRING,
  session_id      STRING,
  device_id       STRING,
  location_id     STRING,
  flow_step       STRING,
  amount_cents    NUMBER,
  currency        STRING,
  plan_type       STRING,
  payment_method  STRING,
  promo_code      STRING,
  renewal_flag    BOOLEAN,
  failure_reason  STRING
);

CREATE OR REPLACE TABLE NOTIFICATION_EVENT (
  event_id            STRING PRIMARY KEY,
  occurred_at         TIMESTAMP_TZ,
  occurred_date       DATE,
  occurred_time       TIME,
  occurred_hour       NUMBER,
  received_at         TIMESTAMP_TZ,
  received_date       DATE,
  received_time       TIME,
  received_hour       NUMBER,
  user_id             STRING,
  household_id        STRING,
  profile_id          STRING,
  session_id          STRING,
  device_id           STRING,
  location_id         STRING,
  notif_id            STRING,
  channel             STRING,
  template_id         STRING,
  campaign_id         STRING,
  sent_ts             TIMESTAMP_TZ,
  opened_ts           TIMESTAMP_TZ,
  cta_clicked         BOOLEAN,
  content_targeted_id STRING
);


--Retention configuration

CREATE OR REPLACE TABLE SILVER_RETENTION_CONFIG (
  table_name      STRING PRIMARY KEY,
  retention_days  NUMBER,
  last_cleanup_ts TIMESTAMP_TZ,
  enabled         BOOLEAN DEFAULT TRUE
);

-- Use explicit columns (avoid ordering mistakes)
DELETE FROM SILVER_RETENTION_CONFIG;
INSERT INTO SILVER_RETENTION_CONFIG (table_name, retention_days, last_cleanup_ts, enabled) VALUES
  ('STREAM_SESSION',     90,  NULL, TRUE),
  ('ENGAGEMENT_EVENT',   90,  NULL, TRUE),
  ('SEARCH_EVENT',       90,  NULL, TRUE),
  ('BROWSE_EVENT',       90,  NULL, TRUE),
  ('TILE_IMPRESSION',    90,  NULL, TRUE),
  ('COMMERCE_EVENT',    365,  NULL, TRUE),
  ('NOTIFICATION_EVENT', 90,  NULL, TRUE);


--Helper function (clean timestamps)

CREATE OR REPLACE FUNCTION DEMO_SILVER.TS_TOL(x VARIANT)
RETURNS TIMESTAMP_TZ
LANGUAGE SQL
AS
$$
  COALESCE(
    -- already a TZ'd string? parse directly
    TRY_TO_TIMESTAMP_TZ(TO_VARCHAR(x)),

    -- no zone provided? try appending UTC indicators
    TRY_TO_TIMESTAMP_TZ(TO_VARCHAR(x) || ' +00:00'),
    TRY_TO_TIMESTAMP_TZ(TO_VARCHAR(x) || 'Z'),

    -- as a last resort: parse as NTZ, then format to ISO and append +00:00
    TRY_TO_TIMESTAMP_TZ(
      TO_CHAR(TO_TIMESTAMP_NTZ(TO_VARCHAR(x)), 'YYYY-MM-DD"T"HH24:MI:SS') || ' +00:00'
    )
  )
$$;



--ONE-TIME BACKFILL (load all)

USE SCHEMA DEMO_BRONZE;

-- Backfill streams expose all existing rows once
CREATE OR REPLACE STREAM STREAM_SESSION_RAW_BF  ON TABLE STREAM_SESSION_RAW  APPEND_ONLY=TRUE SHOW_INITIAL_ROWS=TRUE;
CREATE OR REPLACE STREAM ENGAGEMENT_RAW_BF      ON TABLE ENGAGEMENT_RAW      APPEND_ONLY=TRUE SHOW_INITIAL_ROWS=TRUE;
CREATE OR REPLACE STREAM SEARCH_RAW_BF          ON TABLE SEARCH_RAW          APPEND_ONLY=TRUE SHOW_INITIAL_ROWS=TRUE;
CREATE OR REPLACE STREAM BROWSE_RAW_BF          ON TABLE BROWSE_RAW          APPEND_ONLY=TRUE SHOW_INITIAL_ROWS=TRUE;
CREATE OR REPLACE STREAM TILE_IMPR_RAW_BF       ON TABLE TILE_IMPR_RAW       APPEND_ONLY=TRUE SHOW_INITIAL_ROWS=TRUE;
CREATE OR REPLACE STREAM COMMERCE_RAW_BF        ON TABLE COMMERCE_RAW        APPEND_ONLY=TRUE SHOW_INITIAL_ROWS=TRUE;
CREATE OR REPLACE STREAM NOTIFICATION_RAW_BF    ON TABLE NOTIFICATION_RAW    APPEND_ONLY=TRUE SHOW_INITIAL_ROWS=TRUE;

USE SCHEMA DEMO_SILVER;

-- Load reference tables first (order matters for attribution later)
MERGE INTO STREAM_SESSION t
USING (
  SELECT
    TRIM(r.DATA:"event_id"::string) AS event_id,
    TS_TOL(r.DATA:"occurred_at") AS occurred_at,
    DATE(TS_TOL(r.DATA:"occurred_at")) AS occurred_date,
    TIME(TS_TOL(r.DATA:"occurred_at")) AS occurred_time,
    HOUR(TS_TOL(r.DATA:"occurred_at")) AS occurred_hour,
    r.INGESTED_AT AS received_at,
    DATE(r.INGESTED_AT) AS received_date,
    TIME(r.INGESTED_AT) AS received_time,
    HOUR(r.INGESTED_AT) AS received_hour,
    TRIM(r.DATA:"user_id"::string) AS user_id,
    TRIM(r.DATA:"household_id"::string) AS household_id,
    TRIM(r.DATA:"profile_id"::string) AS profile_id,
    TRIM(r.DATA:"session_id"::string) AS session_id,
    TRIM(r.DATA:"device_id"::string) AS device_id,
    TRIM(r.DATA:"location_id"::string) AS location_id,
    TRIM(r.DATA:"content_id"::string) AS content_id,
    UPPER(NULLIF(r.DATA:"country"::string,'')) AS country,
    TRIM(r.DATA:"experiment_id"::string) AS experiment_id,
    TRIM(r.DATA:"variant"::string) AS variant,
    TS_TOL(r.DATA:"start_ts") AS start_ts,
    TS_TOL(r.DATA:"end_ts") AS end_ts,
    r.DATA:"duration_sec"::number AS duration_sec,
    r.DATA:"playhead_sec"::number AS playhead_sec,
    r.DATA:"avg_bitrate_kbps"::number AS avg_bitrate_kbps,
    r.DATA:"rebuffer_count"::number AS rebuffer_count,
    r.DATA:"rebuffer_sec"::number AS rebuffer_sec,
    r.DATA:"join_time_ms"::number AS join_time_ms,
    r.DATA:"network_type"::string AS network_type,
    r.DATA:"device_model"::string AS device_model
  FROM DEMO_BRONZE.STREAM_SESSION_RAW_BF r
  QUALIFY ROW_NUMBER() OVER (PARTITION BY TRIM(r.DATA:"event_id"::string) ORDER BY r.INGESTED_AT DESC)=1
) s
ON t.event_id = s.event_id
WHEN MATCHED THEN UPDATE SET
  occurred_at=s.occurred_at, occurred_date=s.occurred_date, occurred_time=s.occurred_time, occurred_hour=s.occurred_hour,
  received_at=s.received_at, received_date=s.received_date, received_time=s.received_time, received_hour=s.received_hour,
  user_id=s.user_id, household_id=s.household_id, profile_id=s.profile_id, session_id=s.session_id,
  device_id=s.device_id, location_id=s.location_id, content_id=s.content_id, country=s.country,
  experiment_id=s.experiment_id, variant=s.variant, start_ts=s.start_ts, end_ts=s.end_ts,
  duration_sec=s.duration_sec, playhead_sec=s.playhead_sec, avg_bitrate_kbps=s.avg_bitrate_kbps,
  rebuffer_count=s.rebuffer_count, rebuffer_sec=s.rebuffer_sec, join_time_ms=s.join_time_ms,
  network_type=s.network_type, device_model=s.device_model
WHEN NOT MATCHED THEN INSERT VALUES (
  s.event_id, s.occurred_at, s.occurred_date, s.occurred_time, s.occurred_hour,
  s.received_at, s.received_date, s.received_time, s.received_hour,
  s.user_id, s.household_id, s.profile_id, s.session_id, s.device_id, s.location_id,
  s.content_id, s.country, s.experiment_id, s.variant, s.start_ts, s.end_ts,
  s.duration_sec, s.playhead_sec, s.avg_bitrate_kbps, s.rebuffer_count, s.rebuffer_sec,
  s.join_time_ms, s.network_type, s.device_model
);

MERGE INTO SEARCH_EVENT t
USING (
  SELECT
    TRIM(r.DATA:"event_id"::string) AS event_id,
    TS_TOL(r.DATA:"occurred_at") AS occurred_at,
    DATE(TS_TOL(r.DATA:"occurred_at")) AS occurred_date,
    TIME(TS_TOL(r.DATA:"occurred_at")) AS occurred_time,
    HOUR(TS_TOL(r.DATA:"occurred_at")) AS occurred_hour,
    r.INGESTED_AT AS received_at,
    DATE(r.INGESTED_AT) AS received_date,
    TIME(r.INGESTED_AT) AS received_time,
    HOUR(r.INGESTED_AT) AS received_hour,
    TRIM(r.DATA:"user_id"::string) AS user_id,
    TRIM(r.DATA:"household_id"::string) AS household_id,
    TRIM(r.DATA:"profile_id"::string) AS profile_id,
    TRIM(r.DATA:"session_id"::string) AS session_id,
    TRIM(r.DATA:"device_id"::string) AS device_id,
    TRIM(r.DATA:"location_id"::string) AS location_id,
    r.DATA:"query"::string AS query,
    r.DATA:"results_returned"::number AS results_returned,
    TRIM(r.DATA:"clicked_content_id"::string) AS clicked_content_id,
    r.DATA:"latency_ms"::number AS latency_ms
  FROM DEMO_BRONZE.SEARCH_RAW_BF r
  QUALIFY ROW_NUMBER() OVER (PARTITION BY TRIM(r.DATA:"event_id"::string) ORDER BY r.INGESTED_AT DESC)=1
) s
ON t.event_id = s.event_id
WHEN MATCHED THEN UPDATE SET
  occurred_at=s.occurred_at, occurred_date=s.occurred_date, occurred_time=s.occurred_time, occurred_hour=s.occurred_hour,
  received_at=s.received_at, received_date=s.received_date, received_time=s.received_time, received_hour=s.received_hour,
  user_id=s.user_id, household_id=s.household_id, profile_id=s.profile_id, session_id=s.session_id,
  device_id=s.device_id, location_id=s.location_id,
  query=s.query, results_returned=s.results_returned, clicked_content_id=s.clicked_content_id, latency_ms=s.latency_ms
WHEN NOT MATCHED THEN INSERT VALUES (
  s.event_id, s.occurred_at, s.occurred_date, s.occurred_time, s.occurred_hour,
  s.received_at, s.received_date, s.received_time, s.received_hour,
  s.user_id, s.household_id, s.profile_id, s.session_id, s.device_id, s.location_id,
  s.query, s.results_returned, s.clicked_content_id, s.latency_ms
);

MERGE INTO BROWSE_EVENT t
USING (
  SELECT
    TRIM(r.DATA:"event_id"::string) AS event_id,
    TS_TOL(r.DATA:"occurred_at") AS occurred_at,
    DATE(TS_TOL(r.DATA:"occurred_at")) AS occurred_date,
    TIME(TS_TOL(r.DATA:"occurred_at")) AS occurred_time,
    HOUR(TS_TOL(r.DATA:"occurred_at")) AS occurred_hour,
    r.INGESTED_AT AS received_at,
    DATE(r.INGESTED_AT) AS received_date,
    TIME(r.INGESTED_AT) AS received_time,
    HOUR(r.INGESTED_AT) AS received_hour,
    TRIM(r.DATA:"user_id"::string) AS user_id,
    TRIM(r.DATA:"household_id"::string) AS household_id,
    TRIM(r.DATA:"profile_id"::string) AS profile_id,
    TRIM(r.DATA:"session_id"::string) AS session_id,
    TRIM(r.DATA:"device_id"::string) AS device_id,
    TRIM(r.DATA:"location_id"::string) AS location_id,
    r.DATA:"surface"::string AS surface,
    r.DATA:"view_type"::string AS view_type,
    r.DATA:"nav_action"::string AS nav_action,
    TRIM(r.DATA:"rail_id"::string) AS rail_id,
    r.DATA:"position"::number AS position,
    r.DATA:"impression_value"::number AS impression_value
  FROM DEMO_BRONZE.BROWSE_RAW_BF r
  QUALIFY ROW_NUMBER() OVER (PARTITION BY TRIM(r.DATA:"event_id"::string) ORDER BY r.INGESTED_AT DESC)=1
) s
ON t.event_id = s.event_id
WHEN MATCHED THEN UPDATE SET
  occurred_at=s.occurred_at, occurred_date=s.occurred_date, occurred_time=s.occurred_time, occurred_hour=s.occurred_hour,
  received_at=s.received_at, received_date=s.received_date, received_time=s.received_time, received_hour=s.received_hour,
  user_id=s.user_id, household_id=s.household_id, profile_id=s.profile_id, session_id=s.session_id,
  device_id=s.device_id, location_id=s.location_id, surface=s.surface, view_type=s.view_type,
  nav_action=s.nav_action, rail_id=s.rail_id, position=s.position, impression_value=s.impression_value
WHEN NOT MATCHED THEN INSERT VALUES (
  s.event_id, s.occurred_at, s.occurred_date, s.occurred_time, s.occurred_hour,
  s.received_at, s.received_date, s.received_time, s.received_hour,
  s.user_id, s.household_id, s.profile_id, s.session_id, s.device_id, s.location_id,
  s.surface, s.view_type, s.nav_action, s.rail_id, s.position, s.impression_value
);

MERGE INTO TILE_IMPRESSION t
USING (
  SELECT
    TRIM(r.DATA:"event_id"::string) AS event_id,
    TS_TOL(r.DATA:"occurred_at") AS occurred_at,
    DATE(TS_TOL(r.DATA:"occurred_at")) AS occurred_date,
    TIME(TS_TOL(r.DATA:"occurred_at")) AS occurred_time,
    HOUR(TS_TOL(r.DATA:"occurred_at")) AS occurred_hour,
    r.INGESTED_AT AS received_at,
    DATE(r.INGESTED_AT) AS received_date,
    TIME(r.INGESTED_AT) AS received_time,
    HOUR(r.INGESTED_AT) AS received_hour,
    TRIM(r.DATA:"user_id"::string) AS user_id,
    TRIM(r.DATA:"household_id"::string) AS household_id,
    TRIM(r.DATA:"profile_id"::string) AS profile_id,
    TRIM(r.DATA:"session_id"::string) AS session_id,
    TRIM(r.DATA:"device_id"::string) AS device_id,
    TRIM(r.DATA:"location_id"::string) AS location_id,
    TRIM(r.DATA:"content_id"::string) AS content_id,
    TRIM(r.DATA:"tile_id"::string) AS tile_id,
    TRIM(r.DATA:"row_id"::string) AS row_id,
    r.DATA:"position"::number AS position,
    r.DATA:"dwell_ms"::number AS dwell_ms,
    r.DATA:"preview_watch_ms"::number AS preview_watch_ms,
    r.DATA:"clicked"::boolean AS clicked,
    r.DATA:"conversion_flag"::boolean AS conversion_flag,
    r.DATA:"conversion_latency_sec"::number AS conversion_latency_sec
  FROM DEMO_BRONZE.TILE_IMPR_RAW_BF r
  QUALIFY ROW_NUMBER() OVER (PARTITION BY TRIM(r.DATA:"event_id"::string) ORDER BY r.INGESTED_AT DESC)=1
) s
ON t.event_id = s.event_id
WHEN MATCHED THEN UPDATE SET
  occurred_at=s.occurred_at, occurred_date=s.occurred_date, occurred_time=s.occurred_time, occurred_hour=s.occurred_hour,
  received_at=s.received_at, received_date=s.received_date, received_time=s.received_time, received_hour=s.received_hour,
  user_id=s.user_id, household_id=s.household_id, profile_id=s.profile_id, session_id=s.session_id,
  device_id=s.device_id, location_id=s.location_id, content_id=s.content_id, tile_id=s.tile_id, row_id=s.row_id,
  position=s.position, dwell_ms=s.dwell_ms, preview_watch_ms=s.preview_watch_ms,
  clicked=s.clicked, conversion_flag=s.conversion_flag, conversion_latency_sec=s.conversion_latency_sec
WHEN NOT MATCHED THEN INSERT VALUES (
  s.event_id, s.occurred_at, s.occurred_date, s.occurred_time, s.occurred_hour,
  s.received_at, s.received_date, s.received_time, s.received_hour,
  s.user_id, s.household_id, s.profile_id, s.session_id, s.device_id, s.location_id,
  s.content_id, s.tile_id, s.row_id, s.position, s.dwell_ms, s.preview_watch_ms,
  s.clicked, s.conversion_flag, s.conversion_latency_sec
);

MERGE INTO COMMERCE_EVENT t
USING (
  SELECT
    TRIM(r.DATA:"event_id"::string) AS event_id,
    TS_TOL(r.DATA:"occurred_at") AS occurred_at,
    DATE(TS_TOL(r.DATA:"occurred_at")) AS occurred_date,
    TIME(TS_TOL(r.DATA:"occurred_at")) AS occurred_time,
    HOUR(TS_TOL(r.DATA:"occurred_at")) AS occurred_hour,
    r.INGESTED_AT AS received_at,
    DATE(r.INGESTED_AT) AS received_date,
    TIME(r.INGESTED_AT) AS received_time,
    HOUR(r.INGESTED_AT) AS received_hour,
    TRIM(r.DATA:"user_id"::string) AS user_id,
    TRIM(r.DATA:"household_id"::string) AS household_id,
    TRIM(r.DATA:"profile_id"::string) AS profile_id,
    TRIM(r.DATA:"session_id"::string) AS session_id,
    TRIM(r.DATA:"device_id"::string) AS device_id,
    TRIM(r.DATA:"location_id"::string) AS location_id,
    r.DATA:"flow_step"::string AS flow_step,
    r.DATA:"amount_cents"::number AS amount_cents,
    r.DATA:"currency"::string AS currency,
    r.DATA:"plan_type"::string AS plan_type,
    r.DATA:"payment_method"::string AS payment_method,
    r.DATA:"promo_code"::string AS promo_code,
    r.DATA:"renewal_flag"::boolean AS renewal_flag,
    r.DATA:"failure_reason"::string AS failure_reason
  FROM DEMO_BRONZE.COMMERCE_RAW_BF r
  QUALIFY ROW_NUMBER() OVER (PARTITION BY TRIM(r.DATA:"event_id"::string) ORDER BY r.INGESTED_AT DESC)=1
) s
ON t.event_id = s.event_id
WHEN MATCHED THEN UPDATE SET
  occurred_at=s.occurred_at, occurred_date=s.occurred_date, occurred_time=s.occurred_time, occurred_hour=s.occurred_hour,
  received_at=s.received_at, received_date=s.received_date, received_time=s.received_time, received_hour=s.received_hour,
  user_id=s.user_id, household_id=s.household_id, profile_id=s.profile_id, session_id=s.session_id,
  device_id=s.device_id, location_id=s.location_id, flow_step=s.flow_step, amount_cents=s.amount_cents,
  currency=s.currency, plan_type=s.plan_type, payment_method=s.payment_method, promo_code=s.promo_code,
  renewal_flag=s.renewal_flag, failure_reason=s.failure_reason
WHEN NOT MATCHED THEN INSERT VALUES (
  s.event_id, s.occurred_at, s.occurred_date, s.occurred_time, s.occurred_hour,
  s.received_at, s.received_date, s.received_time, s.received_hour,
  s.user_id, s.household_id, s.profile_id, s.session_id, s.device_id, s.location_id,
  s.flow_step, s.amount_cents, s.currency, s.plan_type, s.payment_method, s.promo_code,
  s.renewal_flag, s.failure_reason
);

MERGE INTO NOTIFICATION_EVENT t
USING (
  SELECT
    TRIM(r.DATA:"event_id"::string) AS event_id,
    TS_TOL(r.DATA:"occurred_at") AS occurred_at,
    DATE(TS_TOL(r.DATA:"occurred_at")) AS occurred_date,
    TIME(TS_TOL(r.DATA:"occurred_at")) AS occurred_time,
    HOUR(TS_TOL(r.DATA:"occurred_at")) AS occurred_hour,
    r.INGESTED_AT AS received_at,
    DATE(r.INGESTED_AT) AS received_date,
    TIME(r.INGESTED_AT) AS received_time,
    HOUR(r.INGESTED_AT) AS received_hour,
    TRIM(r.DATA:"user_id"::string) AS user_id,
    TRIM(r.DATA:"household_id"::string) AS household_id,
    TRIM(r.DATA:"profile_id"::string) AS profile_id,
    TRIM(r.DATA:"session_id"::string) AS session_id,
    TRIM(r.DATA:"device_id"::string) AS device_id,
    TRIM(r.DATA:"location_id"::string) AS location_id,
    TRIM(r.DATA:"notif_id"::string) AS notif_id,
    r.DATA:"channel"::string AS channel,
    TRIM(r.DATA:"template_id"::string) AS template_id,
    TRIM(r.DATA:"campaign_id"::string) AS campaign_id,
    TS_TOL(r.DATA:"sent_ts") AS sent_ts,
    TS_TOL(r.DATA:"opened_ts") AS opened_ts,
    r.DATA:"cta_clicked"::boolean AS cta_clicked,
    TRIM(r.DATA:"content_targeted_id"::string) AS content_targeted_id
  FROM DEMO_BRONZE.NOTIFICATION_RAW_BF r
  QUALIFY ROW_NUMBER() OVER (PARTITION BY TRIM(r.DATA:"event_id"::string) ORDER BY r.INGESTED_AT DESC)=1
) s
ON t.event_id = s.event_id
WHEN MATCHED THEN UPDATE SET
  occurred_at=s.occurred_at, occurred_date=s.occurred_date, occurred_time=s.occurred_time, occurred_hour=s.occurred_hour,
  received_at=s.received_at, received_date=s.received_date, received_time=s.received_time, received_hour=s.received_hour,
  user_id=s.user_id, household_id=s.household_id, profile_id=s.profile_id, session_id=s.session_id,
  device_id=s.device_id, location_id=s.location_id, notif_id=s.notif_id, channel=s.channel, template_id=s.template_id,
  campaign_id=s.campaign_id, sent_ts=s.sent_ts, opened_ts=s.opened_ts, cta_clicked=s.cta_clicked,
  content_targeted_id=s.content_targeted_id
WHEN NOT MATCHED THEN INSERT VALUES (
  s.event_id, s.occurred_at, s.occurred_date, s.occurred_time, s.occurred_hour,
  s.received_at, s.received_date, s.received_time, s.received_hour,
  s.user_id, s.household_id, s.profile_id, s.session_id, s.device_id, s.location_id,
  s.notif_id, s.channel, s.template_id, s.campaign_id, s.sent_ts, s.opened_ts, s.cta_clicked, s.content_targeted_id
);

-- ENGAGEMENT last, with inline attribution (now other tables exist)
MERGE INTO ENGAGEMENT_EVENT t
USING (
  WITH base AS (
    SELECT
      TRIM(r.DATA:"event_id"::string) AS event_id,
      TS_TOL(r.DATA:"occurred_at") AS occurred_at,
      DATE(TS_TOL(r.DATA:"occurred_at")) AS occurred_date,
      TIME(TS_TOL(r.DATA:"occurred_at")) AS occurred_time,
      HOUR(TS_TOL(r.DATA:"occurred_at")) AS occurred_hour,
      r.INGESTED_AT AS received_at,
      DATE(r.INGESTED_AT) AS received_date,
      TIME(r.INGESTED_AT) AS received_time,
      HOUR(r.INGESTED_AT) AS received_hour,
      TRIM(r.DATA:"user_id"::string) AS user_id,
      TRIM(r.DATA:"household_id"::string) AS household_id,
      TRIM(r.DATA:"profile_id"::string) AS profile_id,
      TRIM(r.DATA:"session_id"::string) AS session_id,
      TRIM(r.DATA:"device_id"::string) AS device_id,
      TRIM(r.DATA:"location_id"::string) AS location_id,
      TRIM(r.DATA:"content_id"::string) AS content_id,
      r.DATA:"action"::string AS action,
      r.DATA:"funnel_stage"::string AS funnel_stage,
      r.DATA:"playhead_sec"::number AS playhead_sec,
      r.DATA:"extra" AS extra
    FROM DEMO_BRONZE.ENGAGEMENT_RAW_BF r
    QUALIFY ROW_NUMBER() OVER (PARTITION BY TRIM(r.DATA:"event_id"::string) ORDER BY r.INGESTED_AT DESC)=1
  )
  SELECT
    b.*,
    CASE
      WHEN b.extra:"source"::string = 'tile_click' THEN 'home_row'
      WHEN EXISTS (
        SELECT 1 FROM DEMO_SILVER.SEARCH_EVENT s
        WHERE s.user_id = b.user_id
          AND s.clicked_content_id = b.content_id
          AND s.occurred_at BETWEEN DATEADD(minute,-1,b.occurred_at) AND b.occurred_at
      ) THEN 'search'
      WHEN EXISTS (
        SELECT 1 FROM DEMO_SILVER.NOTIFICATION_EVENT n
        WHERE n.user_id = b.user_id
          AND n.cta_clicked = TRUE
          AND n.occurred_at BETWEEN DATEADD(hour,-6,b.occurred_at) AND b.occurred_at
      ) THEN 'notification'
      WHEN EXISTS (
        SELECT 1 FROM DEMO_SILVER.TILE_IMPRESSION ti
        WHERE ti.user_id = b.user_id
          AND ti.clicked = TRUE
          AND ti.occurred_at BETWEEN DATEADD(minute,-1,b.occurred_at) AND b.occurred_at
      ) THEN 'browse_row'
      ELSE 'unknown'
    END AS play_source
  FROM base b
) s
ON t.event_id = s.event_id
WHEN MATCHED THEN UPDATE SET
  occurred_at=s.occurred_at, occurred_date=s.occurred_date, occurred_time=s.occurred_time, occurred_hour=s.occurred_hour,
  received_at=s.received_at, received_date=s.received_date, received_time=s.received_time, received_hour=s.received_hour,
  user_id=s.user_id, household_id=s.household_id, profile_id=s.profile_id, session_id=s.session_id,
  device_id=s.device_id, location_id=s.location_id, content_id=s.content_id, action=s.action, funnel_stage=s.funnel_stage,
  playhead_sec=s.playhead_sec, extra=s.extra, play_source=s.play_source
WHEN NOT MATCHED THEN INSERT VALUES (
  s.event_id, s.occurred_at, s.occurred_date, s.occurred_time, s.occurred_hour,
  s.received_at, s.received_date, s.received_time, s.received_hour,
  s.user_id, s.household_id, s.profile_id, s.session_id, s.device_id, s.location_id,
  s.content_id, s.action, s.funnel_stage, s.playhead_sec, s.extra, s.play_source
);

-- Drop backfill streams (they were one-time)
USE SCHEMA DEMO_BRONZE;
DROP STREAM IF EXISTS STREAM_SESSION_RAW_BF;
DROP STREAM IF EXISTS ENGAGEMENT_RAW_BF;
DROP STREAM IF EXISTS SEARCH_RAW_BF;
DROP STREAM IF EXISTS BROWSE_RAW_BF;
DROP STREAM IF EXISTS TILE_IMPR_RAW_BF;
DROP STREAM IF EXISTS COMMERCE_RAW_BF;
DROP STREAM IF EXISTS NOTIFICATION_RAW_BF;


--INCREMENTAL STREAMS (do NOT reset)
-- Only create if missing; avoids resetting offsets if already active
CREATE STREAM IF NOT EXISTS STREAM_SESSION_RAW_STM  ON TABLE DEMO_BRONZE.STREAM_SESSION_RAW  APPEND_ONLY=TRUE;
CREATE STREAM IF NOT EXISTS ENGAGEMENT_RAW_STM      ON TABLE DEMO_BRONZE.ENGAGEMENT_RAW      APPEND_ONLY=TRUE;
CREATE STREAM IF NOT EXISTS SEARCH_RAW_STM          ON TABLE DEMO_BRONZE.SEARCH_RAW          APPEND_ONLY=TRUE;
CREATE STREAM IF NOT EXISTS BROWSE_RAW_STM          ON TABLE DEMO_BRONZE.BROWSE_RAW          APPEND_ONLY=TRUE;
CREATE STREAM IF NOT EXISTS TILE_IMPR_RAW_STM       ON TABLE DEMO_BRONZE.TILE_IMPR_RAW       APPEND_ONLY=TRUE;
CREATE STREAM IF NOT EXISTS COMMERCE_RAW_STM        ON TABLE DEMO_BRONZE.COMMERCE_RAW        APPEND_ONLY=TRUE;
CREATE STREAM IF NOT EXISTS NOTIFICATION_RAW_STM    ON TABLE DEMO_BRONZE.NOTIFICATION_RAW    APPEND_ONLY=TRUE;


--INCREMENTAL PROCEDURES (6h)

USE SCHEMA DEMO_SILVER;

CREATE OR REPLACE PROCEDURE SP_SILVER_MERGE()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
  min_recv TIMESTAMP_TZ;
BEGIN
  -- Bound update for attribution later
  SELECT MIN(INGESTED_AT) INTO :min_recv FROM DEMO_BRONZE.ENGAGEMENT_RAW_STM;

  /* STREAM_SESSION */
  MERGE INTO DEMO_SILVER.STREAM_SESSION t
  USING (
    SELECT
      TRIM(r.DATA:"event_id"::string) AS event_id,
      TS_TOL(r.DATA:"occurred_at") AS occurred_at,
      DATE(TS_TOL(r.DATA:"occurred_at")) AS occurred_date,
      TIME(TS_TOL(r.DATA:"occurred_at")) AS occurred_time,
      HOUR(TS_TOL(r.DATA:"occurred_at")) AS occurred_hour,
      r.INGESTED_AT AS received_at,
      DATE(r.INGESTED_AT) AS received_date,
      TIME(r.INGESTED_AT) AS received_time,
      HOUR(r.INGESTED_AT) AS received_hour,
      TRIM(r.DATA:"user_id"::string) AS user_id,
      TRIM(r.DATA:"household_id"::string) AS household_id,
      TRIM(r.DATA:"profile_id"::string) AS profile_id,
      TRIM(r.DATA:"session_id"::string) AS session_id,
      TRIM(r.DATA:"device_id"::string) AS device_id,
      TRIM(r.DATA:"location_id"::string) AS location_id,
      TRIM(r.DATA:"content_id"::string) AS content_id,
      UPPER(NULLIF(r.DATA:"country"::string,'')) AS country,
      TRIM(r.DATA:"experiment_id"::string) AS experiment_id,
      TRIM(r.DATA:"variant"::string) AS variant,
      TS_TOL(r.DATA:"start_ts") AS start_ts,
      TS_TOL(r.DATA:"end_ts") AS end_ts,
      r.DATA:"duration_sec"::number AS duration_sec,
      r.DATA:"playhead_sec"::number AS playhead_sec,
      r.DATA:"avg_bitrate_kbps"::number AS avg_bitrate_kbps,
      r.DATA:"rebuffer_count"::number AS rebuffer_count,
      r.DATA:"rebuffer_sec"::number AS rebuffer_sec,
      r.DATA:"join_time_ms"::number AS join_time_ms,
      r.DATA:"network_type"::string AS network_type,
      r.DATA:"device_model"::string AS device_model
    FROM DEMO_BRONZE.STREAM_SESSION_RAW_STM r
    WHERE TS_TOL(r.DATA:"occurred_at") >= DATEADD(hour,-6, r.INGESTED_AT)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY TRIM(r.DATA:"event_id"::string) ORDER BY r.INGESTED_AT DESC)=1
  ) s
  ON t.event_id = s.event_id
  WHEN MATCHED THEN UPDATE SET
    occurred_at=s.occurred_at, occurred_date=s.occurred_date, occurred_time=s.occurred_time, occurred_hour=s.occurred_hour,
    received_at=s.received_at, received_date=s.received_date, received_time=s.received_time, received_hour=s.received_hour,
    user_id=s.user_id, household_id=s.household_id, profile_id=s.profile_id, session_id=s.session_id,
    device_id=s.device_id, location_id=s.location_id, content_id=s.content_id, country=s.country,
    experiment_id=s.experiment_id, variant=s.variant, start_ts=s.start_ts, end_ts=s.end_ts,
    duration_sec=s.duration_sec, playhead_sec=s.playhead_sec, avg_bitrate_kbps=s.avg_bitrate_kbps,
    rebuffer_count=s.rebuffer_count, rebuffer_sec=s.rebuffer_sec, join_time_ms=s.join_time_ms,
    network_type=s.network_type, device_model=s.device_model
  WHEN NOT MATCHED THEN INSERT VALUES (
    s.event_id, s.occurred_at, s.occurred_date, s.occurred_time, s.occurred_hour,
    s.received_at, s.received_date, s.received_time, s.received_hour,
    s.user_id, s.household_id, s.profile_id, s.session_id, s.device_id, s.location_id,
    s.content_id, s.country, s.experiment_id, s.variant, s.start_ts, s.end_ts,
    s.duration_sec, s.playhead_sec, s.avg_bitrate_kbps, s.rebuffer_count, s.rebuffer_sec,
    s.join_time_ms, s.network_type, s.device_model
  );

  /* SEARCH_EVENT */
  MERGE INTO DEMO_SILVER.SEARCH_EVENT t
  USING (
    SELECT
      TRIM(r.DATA:"event_id"::string) AS event_id,
      TS_TOL(r.DATA:"occurred_at") AS occurred_at,
      DATE(TS_TOL(r.DATA:"occurred_at")) AS occurred_date,
      TIME(TS_TOL(r.DATA:"occurred_at")) AS occurred_time,
      HOUR(TS_TOL(r.DATA:"occurred_at")) AS occurred_hour,
      r.INGESTED_AT AS received_at,
      DATE(r.INGESTED_AT) AS received_date,
      TIME(r.INGESTED_AT) AS received_time,
      HOUR(r.INGESTED_AT) AS received_hour,
      TRIM(r.DATA:"user_id"::string) AS user_id,
      TRIM(r.DATA:"household_id"::string) AS household_id,
      TRIM(r.DATA:"profile_id"::string) AS profile_id,
      TRIM(r.DATA:"session_id"::string) AS session_id,
      TRIM(r.DATA:"device_id"::string) AS device_id,
      TRIM(r.DATA:"location_id"::string) AS location_id,
      r.DATA:"query"::string AS query,
      r.DATA:"results_returned"::number AS results_returned,
      TRIM(r.DATA:"clicked_content_id"::string) AS clicked_content_id,
      r.DATA:"latency_ms"::number AS latency_ms
    FROM DEMO_BRONZE.SEARCH_RAW_STM r
    WHERE TS_TOL(r.DATA:"occurred_at") >= DATEADD(hour,-6, r.INGESTED_AT)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY TRIM(r.DATA:"event_id"::string) ORDER BY r.INGESTED_AT DESC)=1
  ) s
  ON t.event_id = s.event_id
  WHEN MATCHED THEN UPDATE SET
    occurred_at=s.occurred_at, occurred_date=s.occurred_date, occurred_time=s.occurred_time, occurred_hour=s.occurred_hour,
    received_at=s.received_at, received_date=s.received_date, received_time=s.received_time, received_hour=s.received_hour,
    user_id=s.user_id, household_id=s.household_id, profile_id=s.profile_id, session_id=s.session_id,
    device_id=s.device_id, location_id=s.location_id,
    query=s.query, results_returned=s.results_returned, clicked_content_id=s.clicked_content_id, latency_ms=s.latency_ms
  WHEN NOT MATCHED THEN INSERT VALUES (
    s.event_id, s.occurred_at, s.occurred_date, s.occurred_time, s.occurred_hour,
    s.received_at, s.received_date, s.received_time, s.received_hour, s.user_id, s.household_id,
    s.profile_id, s.session_id, s.device_id, s.location_id, s.query, s.results_returned, s.clicked_content_id, s.latency_ms
  );

  /* BROWSE_EVENT */
  MERGE INTO DEMO_SILVER.BROWSE_EVENT t
  USING (
    SELECT
      TRIM(r.DATA:"event_id"::string) AS event_id,
      TS_TOL(r.DATA:"occurred_at") AS occurred_at,
      DATE(TS_TOL(r.DATA:"occurred_at")) AS occurred_date,
      TIME(TS_TOL(r.DATA:"occurred_at")) AS occurred_time,
      HOUR(TS_TOL(r.DATA:"occurred_at")) AS occurred_hour,
      r.INGESTED_AT AS received_at,
      DATE(r.INGESTED_AT) AS received_date,
      TIME(r.INGESTED_AT) AS received_time,
      HOUR(r.INGESTED_AT) AS received_hour,
      TRIM(r.DATA:"user_id"::string) AS user_id,
      TRIM(r.DATA:"household_id"::string) AS household_id,
      TRIM(r.DATA:"profile_id"::string) AS profile_id,
      TRIM(r.DATA:"session_id"::string) AS session_id,
      TRIM(r.DATA:"device_id"::string) AS device_id,
      TRIM(r.DATA:"location_id"::string) AS location_id,
      r.DATA:"surface"::string AS surface,
      r.DATA:"view_type"::string AS view_type,
      r.DATA:"nav_action"::string AS nav_action,
      TRIM(r.DATA:"rail_id"::string) AS rail_id,
      r.DATA:"position"::number AS position,
      r.DATA:"impression_value"::number AS impression_value
    FROM DEMO_BRONZE.BROWSE_RAW_STM r
    WHERE TS_TOL(r.DATA:"occurred_at") >= DATEADD(hour,-6, r.INGESTED_AT)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY TRIM(r.DATA:"event_id"::string) ORDER BY r.INGESTED_AT DESC)=1
  ) s
  ON t.event_id = s.event_id
  WHEN MATCHED THEN UPDATE SET
    occurred_at=s.occurred_at, occurred_date=s.occurred_date, occurred_time=s.occurred_time, occurred_hour=s.occurred_hour,
    received_at=s.received_at, received_date=s.received_date, received_time=s.received_time, received_hour=s.received_hour,
    user_id=s.user_id, household_id=s.household_id, profile_id=s.profile_id, session_id=s.session_id,
    device_id=s.device_id, location_id=s.location_id, surface=s.surface, view_type=s.view_type,
    nav_action=s.nav_action, rail_id=s.rail_id, position=s.position, impression_value=s.impression_value
  WHEN NOT MATCHED THEN INSERT VALUES (
    s.event_id, s.occurred_at, s.occurred_date, s.occurred_time, s.occurred_hour,
    s.received_at, s.received_date, s.received_time, s.received_hour, s.user_id, s.household_id,
    s.profile_id, s.session_id, s.device_id, s.location_id, s.surface, s.view_type, s.nav_action, s.rail_id, s.position, s.impression_value
  );

  /* TILE_IMPRESSION */
  MERGE INTO DEMO_SILVER.TILE_IMPRESSION t
  USING (
    SELECT
      TRIM(r.DATA:"event_id"::string) AS event_id,
      TS_TOL(r.DATA:"occurred_at") AS occurred_at,
      DATE(TS_TOL(r.DATA:"occurred_at")) AS occurred_date,
      TIME(TS_TOL(r.DATA:"occurred_at")) AS occurred_time,
      HOUR(TS_TOL(r.DATA:"occurred_at")) AS occurred_hour,
      r.INGESTED_AT AS received_at,
      DATE(r.INGESTED_AT) AS received_date,
      TIME(r.INGESTED_AT) AS received_time,
      HOUR(r.INGESTED_AT) AS received_hour,
      TRIM(r.DATA:"user_id"::string) AS user_id,
      TRIM(r.DATA:"household_id"::string) AS household_id,
      TRIM(r.DATA:"profile_id"::string) AS profile_id,
      TRIM(r.DATA:"session_id"::string) AS session_id,
      TRIM(r.DATA:"device_id"::string) AS device_id,
      TRIM(r.DATA:"location_id"::string) AS location_id,
      TRIM(r.DATA:"content_id"::string) AS content_id,
      TRIM(r.DATA:"tile_id"::string) AS tile_id,
      TRIM(r.DATA:"row_id"::string) AS row_id,
      r.DATA:"position"::number AS position,
      r.DATA:"dwell_ms"::number AS dwell_ms,
      r.DATA:"preview_watch_ms"::number AS preview_watch_ms,
      r.DATA:"clicked"::boolean AS clicked,
      r.DATA:"conversion_flag"::boolean AS conversion_flag,
      r.DATA:"conversion_latency_sec"::number AS conversion_latency_sec
    FROM DEMO_BRONZE.TILE_IMPR_RAW_STM r
    WHERE TS_TOL(r.DATA:"occurred_at") >= DATEADD(hour,-6, r.INGESTED_AT)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY TRIM(r.DATA:"event_id"::string) ORDER BY r.INGESTED_AT DESC)=1
  ) s
  ON t.event_id = s.event_id
  WHEN MATCHED THEN UPDATE SET
    occurred_at=s.occurred_at, occurred_date=s.occurred_date, occurred_time=s.occurred_time, occurred_hour=s.occurred_hour,
    received_at=s.received_at, received_date=s.received_date, received_time=s.received_time, received_hour=s.received_hour,
    user_id=s.user_id, household_id=s.household_id, profile_id=s.profile_id, session_id=s.session_id,
    device_id=s.device_id, location_id=s.location_id, content_id=s.content_id, tile_id=s.tile_id, row_id=s.row_id,
    position=s.position, dwell_ms=s.dwell_ms, preview_watch_ms=s.preview_watch_ms,
    clicked=s.clicked, conversion_flag=s.conversion_flag, conversion_latency_sec=s.conversion_latency_sec
  WHEN NOT MATCHED THEN INSERT VALUES (
    s.event_id, s.occurred_at, s.occurred_date, s.occurred_time, s.occurred_hour,
    s.received_at, s.received_date, s.received_time, s.received_hour, s.user_id, s.household_id,
    s.profile_id, s.session_id, s.device_id, s.location_id, s.content_id, s.tile_id, s.row_id,
    s.position, s.dwell_ms, s.preview_watch_ms, s.clicked, s.conversion_flag, s.conversion_latency_sec
  );

  /* COMMERCE_EVENT */
  MERGE INTO DEMO_SILVER.COMMERCE_EVENT t
  USING (
    SELECT
      TRIM(r.DATA:"event_id"::string) AS event_id,
      TS_TOL(r.DATA:"occurred_at") AS occurred_at,
      DATE(TS_TOL(r.DATA:"occurred_at")) AS occurred_date,
      TIME(TS_TOL(r.DATA:"occurred_at")) AS occurred_time,
      HOUR(TS_TOL(r.DATA:"occurred_at")) AS occurred_hour,
      r.INGESTED_AT AS received_at,
      DATE(r.INGESTED_AT) AS received_date,
      TIME(r.INGESTED_AT) AS received_time,
      HOUR(r.INGESTED_AT) AS received_hour,
      TRIM(r.DATA:"user_id"::string) AS user_id,
      TRIM(r.DATA:"household_id"::string) AS household_id,
      TRIM(r.DATA:"profile_id"::string) AS profile_id,
      TRIM(r.DATA:"session_id"::string) AS session_id,
      TRIM(r.DATA:"device_id"::string) AS device_id,
      TRIM(r.DATA:"location_id"::string) AS location_id,
      r.DATA:"flow_step"::string AS flow_step,
      r.DATA:"amount_cents"::number AS amount_cents,
      r.DATA:"currency"::string AS currency,
      r.DATA:"plan_type"::string AS plan_type,
      r.DATA:"payment_method"::string AS payment_method,
      r.DATA:"promo_code"::string AS promo_code,
      r.DATA:"renewal_flag"::boolean AS renewal_flag,
      r.DATA:"failure_reason"::string AS failure_reason
    FROM DEMO_BRONZE.COMMERCE_RAW_STM r
    WHERE TS_TOL(r.DATA:"occurred_at") >= DATEADD(hour,-6, r.INGESTED_AT)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY TRIM(r.DATA:"event_id"::string) ORDER BY r.INGESTED_AT DESC)=1
  ) s
  ON t.event_id = s.event_id
  WHEN MATCHED THEN UPDATE SET
    occurred_at=s.occurred_at, occurred_date=s.occurred_date, occurred_time=s.occurred_time, occurred_hour=s.occurred_hour,
    received_at=s.received_at, received_date=s.received_date, received_time=s.received_time, received_hour=s.received_hour,
    user_id=s.user_id, household_id=s.household_id, profile_id=s.profile_id, session_id=s.session_id,
    device_id=s.device_id, location_id=s.location_id, flow_step=s.flow_step, amount_cents=s.amount_cents,
    currency=s.currency, plan_type=s.plan_type, payment_method=s.payment_method, promo_code=s.promo_code,
    renewal_flag=s.renewal_flag, failure_reason=s.failure_reason
  WHEN NOT MATCHED THEN INSERT VALUES (
    s.event_id, s.occurred_at, s.occurred_date, s.occurred_time, s.occurred_hour,
    s.received_at, s.received_date, s.received_time, s.received_hour, s.user_id, s.household_id,
    s.profile_id, s.session_id, s.device_id, s.location_id, s.flow_step, s.amount_cents, s.currency,
    s.plan_type, s.payment_method, s.promo_code, s.renewal_flag, s.failure_reason
  );

  /* NOTIFICATION_EVENT */
  MERGE INTO DEMO_SILVER.NOTIFICATION_EVENT t
  USING (
    SELECT
      TRIM(r.DATA:"event_id"::string) AS event_id,
      TS_TOL(r.DATA:"occurred_at") AS occurred_at,
      DATE(TS_TOL(r.DATA:"occurred_at")) AS occurred_date,
      TIME(TS_TOL(r.DATA:"occurred_at")) AS occurred_time,
      HOUR(TS_TOL(r.DATA:"occurred_at")) AS occurred_hour,
      r.INGESTED_AT AS received_at,
      DATE(r.INGESTED_AT) AS received_date,
      TIME(r.INGESTED_AT) AS received_time,
      HOUR(r.INGESTED_AT) AS received_hour,
      TRIM(r.DATA:"user_id"::string) AS user_id,
      TRIM(r.DATA:"household_id"::string) AS household_id,
      TRIM(r.DATA:"profile_id"::string) AS profile_id,
      TRIM(r.DATA:"session_id"::string) AS session_id,
      TRIM(r.DATA:"device_id"::string) AS device_id,
      TRIM(r.DATA:"location_id"::string) AS location_id,
      TRIM(r.DATA:"notif_id"::string) AS notif_id,
      r.DATA:"channel"::string AS channel,
      TRIM(r.DATA:"template_id"::string) AS template_id,
      TRIM(r.DATA:"campaign_id"::string) AS campaign_id,
      TS_TOL(r.DATA:"sent_ts") AS sent_ts,
      TS_TOL(r.DATA:"opened_ts") AS opened_ts,
      r.DATA:"cta_clicked"::boolean AS cta_clicked,
      TRIM(r.DATA:"content_targeted_id"::string) AS content_targeted_id
    FROM DEMO_BRONZE.NOTIFICATION_RAW_STM r
    WHERE TS_TOL(r.DATA:"occurred_at") >= DATEADD(hour,-6, r.INGESTED_AT)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY TRIM(r.DATA:"event_id"::string) ORDER BY r.INGESTED_AT DESC)=1
  ) s
  ON t.event_id = s.event_id
  WHEN MATCHED THEN UPDATE SET
    occurred_at=s.occurred_at, occurred_date=s.occurred_date, occurred_time=s.occurred_time, occurred_hour=s.occurred_hour,
    received_at=s.received_at, received_date=s.received_date, received_time=s.received_time, received_hour=s.received_hour,
    user_id=s.user_id, household_id=s.household_id, profile_id=s.profile_id, session_id=s.session_id,
    device_id=s.device_id, location_id=s.location_id, notif_id=s.notif_id, channel=s.channel, template_id=s.template_id,
    campaign_id=s.campaign_id, sent_ts=s.sent_ts, opened_ts=s.opened_ts, cta_clicked=s.cta_clicked, content_targeted_id=s.content_targeted_id
  WHEN NOT MATCHED THEN INSERT VALUES (
    s.event_id, s.occurred_at, s.occurred_date, s.occurred_time, s.occurred_hour,
    s.received_at, s.received_date, s.received_time, s.received_hour, s.user_id, s.household_id,
    s.profile_id, s.session_id, s.device_id, s.location_id, s.notif_id, s.channel, s.template_id,
    s.campaign_id, s.sent_ts, s.opened_ts, s.cta_clicked, s.content_targeted_id
  );

  /* ENGAGEMENT_EVENT upsert (no attribution yet) */
  MERGE INTO DEMO_SILVER.ENGAGEMENT_EVENT t
  USING (
    SELECT
      TRIM(r.DATA:"event_id"::string) AS event_id,
      TS_TOL(r.DATA:"occurred_at") AS occurred_at,
      DATE(TS_TOL(r.DATA:"occurred_at")) AS occurred_date,
      TIME(TS_TOL(r.DATA:"occurred_at")) AS occurred_time,
      HOUR(TS_TOL(r.DATA:"occurred_at")) AS occurred_hour,
      r.INGESTED_AT AS received_at,
      DATE(r.INGESTED_AT) AS received_date,
      TIME(r.INGESTED_AT) AS received_time,
      HOUR(r.INGESTED_AT) AS received_hour,
      TRIM(r.DATA:"user_id"::string) AS user_id,
      TRIM(r.DATA:"household_id"::string) AS household_id,
      TRIM(r.DATA:"profile_id"::string) AS profile_id,
      TRIM(r.DATA:"session_id"::string) AS session_id,
      TRIM(r.DATA:"device_id"::string) AS device_id,
      TRIM(r.DATA:"location_id"::string) AS location_id,
      TRIM(r.DATA:"content_id"::string) AS content_id,
      r.DATA:"action"::string AS action,
      r.DATA:"funnel_stage"::string AS funnel_stage,
      r.DATA:"playhead_sec"::number AS playhead_sec,
      r.DATA:"extra" AS extra
    FROM DEMO_BRONZE.ENGAGEMENT_RAW_STM r
    WHERE TS_TOL(r.DATA:"occurred_at") >= DATEADD(hour,-6, r.INGESTED_AT)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY TRIM(r.DATA:"event_id"::string) ORDER BY r.INGESTED_AT DESC)=1
  ) s
  ON t.event_id = s.event_id
  WHEN MATCHED THEN UPDATE SET
    occurred_at=s.occurred_at, occurred_date=s.occurred_date, occurred_time=s.occurred_time, occurred_hour=s.occurred_hour,
    received_at=s.received_at, received_date=s.received_date, received_time=s.received_time, received_hour=s.received_hour,
    user_id=s.user_id, household_id=s.household_id, profile_id=s.profile_id, session_id=s.session_id,
    device_id=s.device_id, location_id=s.location_id, content_id=s.content_id,
    action=s.action, funnel_stage=s.funnel_stage, playhead_sec=s.playhead_sec, extra=s.extra
  WHEN NOT MATCHED THEN INSERT VALUES (
    s.event_id, s.occurred_at, s.occurred_date, s.occurred_time, s.occurred_hour,
    s.received_at, s.received_date, s.received_time, s.received_hour, s.user_id, s.household_id,
    s.profile_id, s.session_id, s.device_id, s.location_id, s.content_id,
    s.action, s.funnel_stage, s.playhead_sec, s.extra, NULL
  );

  -- Attribution update for just-arrived rows
  IF (min_recv IS NOT NULL) THEN
    UPDATE DEMO_SILVER.ENGAGEMENT_EVENT e
    SET play_source = CASE
      WHEN e.extra:"source"::string = 'tile_click' THEN 'home_row'
      WHEN EXISTS (
        SELECT 1 FROM DEMO_SILVER.SEARCH_EVENT s
        WHERE s.user_id = e.user_id
          AND s.clicked_content_id = e.content_id
          AND s.occurred_at BETWEEN DATEADD(minute,-1,e.occurred_at) AND e.occurred_at
      ) THEN 'search'
      WHEN EXISTS (
        SELECT 1 FROM DEMO_SILVER.NOTIFICATION_EVENT n
        WHERE n.user_id = e.user_id
          AND n.cta_clicked = TRUE
          AND n.occurred_at BETWEEN DATEADD(hour,-6,e.occurred_at) AND e.occurred_at
      ) THEN 'notification'
      WHEN EXISTS (
        SELECT 1 FROM DEMO_SILVER.TILE_IMPRESSION ti
        WHERE ti.user_id = e.user_id
          AND ti.clicked = TRUE
          AND ti.occurred_at BETWEEN DATEADD(minute,-1,e.occurred_at) AND e.occurred_at
      ) THEN 'browse_row'
      ELSE 'unknown'
    END
    WHERE e.received_at >= min_recv
      AND (e.play_source IS NULL OR e.play_source = 'unknown');
  END IF;

  RETURN 'Silver merge OK';
END;
$$;

CREATE OR REPLACE PROCEDURE SP_SILVER_ATTRIBUTION_BACKFILL()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
  UPDATE DEMO_SILVER.ENGAGEMENT_EVENT e
  SET play_source = CASE
    WHEN e.extra:"source"::string = 'tile_click' THEN 'home_row'
    WHEN EXISTS (
      SELECT 1 FROM DEMO_SILVER.SEARCH_EVENT s
      WHERE s.user_id = e.user_id
        AND s.clicked_content_id = e.content_id
        AND s.occurred_at BETWEEN DATEADD(minute,-1,e.occurred_at) AND e.occurred_at
    ) THEN 'search'
    WHEN EXISTS (
      SELECT 1 FROM DEMO_SILVER.NOTIFICATION_EVENT n
      WHERE n.user_id = e.user_id
        AND n.cta_clicked = TRUE
        AND n.occurred_at BETWEEN DATEADD(hour,-6,e.occurred_at) AND e.occurred_at
    ) THEN 'notification'
    WHEN EXISTS (
      SELECT 1 FROM DEMO_SILVER.TILE_IMPRESSION ti
      WHERE ti.user_id = e.user_id
        AND ti.clicked = TRUE
        AND ti.occurred_at BETWEEN DATEADD(minute,-1,e.occurred_at) AND e.occurred_at
    ) THEN 'browse_row'
    ELSE 'unknown'
  END
  WHERE e.play_source IS NULL OR e.play_source = 'unknown';

  RETURN 'Attribution refresh OK';
END;
$$;


--Retention cleanup (SILVER)

CREATE OR REPLACE PROCEDURE DEMO_SILVER.SP_SILVER_CLEANUP()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
  tbl STRING;
  days NUMBER;
  del NUMBER;
  msg STRING DEFAULT '';
  cur CURSOR FOR
    SELECT table_name, retention_days
    FROM DEMO_SILVER.SILVER_RETENTION_CONFIG
    WHERE enabled = TRUE;
BEGIN
  FOR r IN cur DO
    tbl := r.table_name;
    days := r.retention_days;

    -- Build dynamic SQL without IDENTIFIER()
    EXECUTE IMMEDIATE
      'DELETE FROM DEMO_SILVER.' || tbl ||
      ' WHERE occurred_date < DATEADD(day, -' || days || ', CURRENT_DATE())';

    del := SQLROWCOUNT;

    UPDATE DEMO_SILVER.SILVER_RETENTION_CONFIG
      SET last_cleanup_ts = CURRENT_TIMESTAMP()
      WHERE table_name = tbl;

    msg := msg || tbl || ': ' || del || ' deleted; ';
  END FOR;

  RETURN msg;
END;
$$;


--Tasks (Sydney TZ)

CREATE OR REPLACE TASK T_SILVER_MERGE
  WAREHOUSE = GOPHER_WH
  SCHEDULE = 'USING CRON */5 * * * * Australia/Sydney'
AS
CALL SP_SILVER_MERGE();

CREATE OR REPLACE TASK T_SILVER_ATTRIBUTION
  WAREHOUSE = GOPHER_WH
  SCHEDULE = 'USING CRON 0 */6 * * * Australia/Sydney'
AS
CALL SP_SILVER_ATTRIBUTION_BACKFILL();

CREATE OR REPLACE TASK T_SILVER_CLEANUP
  WAREHOUSE = GOPHER_WH
  SCHEDULE = 'USING CRON 0 2 * * * Australia/Sydney'
AS
CALL SP_SILVER_CLEANUP();

ALTER TASK T_SILVER_MERGE RESUME;
ALTER TASK T_SILVER_ATTRIBUTION RESUME;
ALTER TASK T_SILVER_CLEANUP RESUME;


--Utility views

CREATE OR REPLACE VIEW V_SESSION_FRICTION_INPUTS AS
SELECT
  user_id, content_id,
  occurred_at, occurred_date, occurred_hour,
  join_time_ms, rebuffer_sec, rebuffer_count
FROM DEMO_SILVER.STREAM_SESSION
WHERE join_time_ms IS NOT NULL;

CREATE OR REPLACE VIEW V_EXPOSURE_SEQUENCE AS
SELECT
  user_id, content_id,
  occurred_at, occurred_date, occurred_hour,
  ROW_NUMBER() OVER (PARTITION BY user_id, content_id ORDER BY occurred_at) AS exposure_seq
FROM DEMO_SILVER.TILE_IMPRESSION;

CREATE OR REPLACE VIEW V_HOURLY_ENGAGEMENT_METRICS AS
SELECT
  occurred_date,
  occurred_hour,
  action,
  play_source,
  COUNT(*) AS event_count,
  COUNT(DISTINCT user_id) AS unique_users,
  COUNT(DISTINCT session_id) AS unique_sessions
FROM DEMO_SILVER.ENGAGEMENT_EVENT
GROUP BY occurred_date, occurred_hour, action, play_source;

CREATE OR REPLACE VIEW V_RETENTION_STATUS AS
SELECT
  table_name,
  retention_days,
  last_cleanup_ts,
  DATEDIFF(day, last_cleanup_ts, CURRENT_TIMESTAMP()) AS days_since_cleanup,
  enabled
FROM DEMO_SILVER.SILVER_RETENTION_CONFIG
ORDER BY table_name;

CREATE OR REPLACE VIEW V_DATA_QUALITY_LATE_ARRIVALS AS
WITH eng AS (
  SELECT
    occurred_date,
    COUNT(*) AS total_events,
    SUM(IFF(DATEDIFF(minute, occurred_at, received_at) > 30, 1, 0)) AS late_arrivals
  FROM DEMO_SILVER.ENGAGEMENT_EVENT
  WHERE occurred_date >= DATEADD(day, -7, CURRENT_DATE())
  GROUP BY occurred_date
),
srch AS (
  SELECT
    occurred_date,
    COUNT(*) AS total_events,
    SUM(IFF(DATEDIFF(minute, occurred_at, received_at) > 30, 1, 0)) AS late_arrivals
  FROM DEMO_SILVER.SEARCH_EVENT
  WHERE occurred_date >= DATEADD(day, -7, CURRENT_DATE())
  GROUP BY occurred_date
)
SELECT
  occurred_date,
  'ENGAGEMENT' AS event_type,
  total_events,
  late_arrivals,
  ROUND(100.0 * late_arrivals / NULLIF(total_events, 0), 2) AS late_arrival_pct
FROM eng
UNION ALL
SELECT
  occurred_date,
  'SEARCH' AS event_type,
  total_events,
  late_arrivals,
  ROUND(100.0 * late_arrivals / NULLIF(total_events, 0), 2) AS late_arrival_pct
FROM srch
ORDER BY occurred_date DESC, event_type;



