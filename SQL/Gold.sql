-- ================================
-- GOLD LAYER (clean star + KPI views)
-- ================================
USE WAREHOUSE GOPHER_WH;
USE DATABASE GOPHER_DB;
CREATE SCHEMA IF NOT EXISTS DEMO_GOLD;
USE SCHEMA DEMO_GOLD;

-- ---------- Helper functions ----------
CREATE OR REPLACE FUNCTION SK_STR(s STRING)
RETURNS STRING
LANGUAGE SQL
STRICT
AS $$
  LOWER(TO_VARCHAR(MD5(COALESCE(s, ''))))
$$;

CREATE OR REPLACE FUNCTION SK_2STR(a STRING, b STRING)
RETURNS STRING
LANGUAGE SQL
STRICT
AS $$
  LOWER(TO_VARCHAR(MD5(COALESCE(a,'') || '|' || COALESCE(b,''))))
$$;

CREATE OR REPLACE FUNCTION SK_TIME_HOUR(ts TIMESTAMP_TZ)
RETURNS STRING
LANGUAGE SQL
AS $$
  DEMO_GOLD.SK_STR(
    TO_CHAR(DATE_TRUNC('hour', ts), 'YYYY-MM-DD HH24:00:00 TZH:TZM')
  )
$$;

-- ===================== DIMENSIONS =====================
CREATE OR REPLACE TABLE DIM_USER_T AS
SELECT
  DEMO_GOLD.SK_STR(user_id) AS user_sk,
  user_id,
  ANY_VALUE(country)        AS country
FROM DEMO_SILVER.STREAM_SESSION
GROUP BY user_id;

CREATE OR REPLACE TABLE DIM_CONTENT_T AS
SELECT
  DEMO_GOLD.SK_STR(content_id) AS content_sk,
  content_id
FROM (
  SELECT content_id FROM DEMO_SILVER.STREAM_SESSION
  UNION DISTINCT
  SELECT content_id FROM DEMO_SILVER.TILE_IMPRESSION
  UNION DISTINCT
  SELECT clicked_content_id AS content_id
  FROM DEMO_SILVER.SEARCH_EVENT WHERE clicked_content_id IS NOT NULL
);

CREATE OR REPLACE TABLE DIM_DEVICE_T AS
SELECT
  DEMO_GOLD.SK_STR(device_model) AS device_sk,
  device_model
FROM DEMO_SILVER.STREAM_SESSION
WHERE device_model IS NOT NULL
GROUP BY device_model;

CREATE OR REPLACE TABLE DIM_LOCATION_T AS
SELECT
  DEMO_GOLD.SK_STR(location_id) AS location_sk,
  location_id,
  ANY_VALUE(country)            AS country
FROM DEMO_SILVER.STREAM_SESSION
WHERE location_id IS NOT NULL
GROUP BY location_id;

CREATE OR REPLACE TABLE DIM_TIME_T AS
WITH hours AS (
  SELECT DISTINCT DATE_TRUNC('hour', occurred_at) AS hr FROM (
    SELECT occurred_at FROM DEMO_SILVER.STREAM_SESSION
    UNION ALL SELECT occurred_at FROM DEMO_SILVER.ENGAGEMENT_EVENT
    UNION ALL SELECT occurred_at FROM DEMO_SILVER.TILE_IMPRESSION
    UNION ALL SELECT occurred_at FROM DEMO_SILVER.SEARCH_EVENT
    UNION ALL SELECT occurred_at FROM DEMO_SILVER.NOTIFICATION_EVENT
    UNION ALL SELECT occurred_at FROM DEMO_SILVER.COMMERCE_EVENT
  )
)
SELECT
  DEMO_GOLD.SK_TIME_HOUR(hr) AS time_sk,
  hr                         AS occurred_hour,
  TO_DATE(hr)                AS occurred_date,
  YEAR(hr)                   AS year,
  MONTH(hr)                  AS month,
  DAY(hr)                    AS day,
  DAYOFWEEKISO(hr)           AS dow_iso,
  TO_CHAR(hr,'DY')           AS dow_short,
  HOUR(hr)                   AS hour
FROM hours;

-- ======================= FACTS ========================
CREATE OR REPLACE TABLE FACT_SESSION_T AS
SELECT
  DEMO_GOLD.SK_STR(ss.user_id)            AS user_sk,
  DEMO_GOLD.SK_STR(ss.content_id)         AS content_sk,
  DEMO_GOLD.SK_STR(ss.device_model)       AS device_sk,
  DEMO_GOLD.SK_STR(ss.location_id)        AS location_sk,
  DEMO_GOLD.SK_TIME_HOUR(ss.occurred_at)  AS time_sk,
  ss.event_id, ss.session_id, ss.occurred_at,
  ss.duration_sec, ss.playhead_sec, ss.avg_bitrate_kbps,
  ss.rebuffer_count, ss.rebuffer_sec, ss.join_time_ms
FROM DEMO_SILVER.STREAM_SESSION ss;

CREATE OR REPLACE TABLE FACT_ENGAGEMENT_T AS
SELECT
  DEMO_GOLD.SK_STR(e.user_id)             AS user_sk,
  DEMO_GOLD.SK_STR(e.content_id)          AS content_sk,
  DEMO_GOLD.SK_TIME_HOUR(e.occurred_at)   AS time_sk,
  e.event_id, e.session_id, e.occurred_at,
  e.action, e.funnel_stage, e.playhead_sec, e.play_source
FROM DEMO_SILVER.ENGAGEMENT_EVENT e;

CREATE OR REPLACE TABLE FACT_TILE_IMPRESSION_T AS
SELECT
  DEMO_GOLD.SK_STR(t.user_id)             AS user_sk,
  DEMO_GOLD.SK_STR(t.content_id)          AS content_sk,
  DEMO_GOLD.SK_TIME_HOUR(t.occurred_at)   AS time_sk,
  t.event_id, t.session_id, t.occurred_at,
  t.row_id, t.position, t.dwell_ms, t.preview_watch_ms,
  t.clicked, t.conversion_flag, t.conversion_latency_sec
FROM DEMO_SILVER.TILE_IMPRESSION t;

CREATE OR REPLACE TABLE FACT_SEARCH_T AS
SELECT
  DEMO_GOLD.SK_STR(s.user_id)             AS user_sk,
  DEMO_GOLD.SK_TIME_HOUR(s.occurred_at)   AS time_sk,
  s.event_id, s.session_id, s.occurred_at,
  s.query, s.results_returned, s.clicked_content_id
FROM DEMO_SILVER.SEARCH_EVENT s;

CREATE OR REPLACE TABLE FACT_NOTIFICATION_T AS
SELECT
  DEMO_GOLD.SK_STR(n.user_id)             AS user_sk,
  DEMO_GOLD.SK_TIME_HOUR(n.occurred_at)   AS time_sk,
  n.event_id, n.session_id, n.occurred_at,
  n.channel, n.template_id, n.campaign_id,
  n.sent_ts, n.opened_ts, n.cta_clicked, n.content_targeted_id
FROM DEMO_SILVER.NOTIFICATION_EVENT n;

CREATE OR REPLACE TABLE FACT_COMMERCE_T AS
SELECT
  DEMO_GOLD.SK_STR(c.user_id)             AS user_sk,
  DEMO_GOLD.SK_TIME_HOUR(c.occurred_at)   AS time_sk,
  c.event_id, c.session_id, c.occurred_at,
  c.flow_step, c.amount_cents, c.currency,
  c.plan_type, c.payment_method, c.promo_code,
  c.renewal_flag, c.failure_reason
FROM DEMO_SILVER.COMMERCE_EVENT c;



-- =================== KPI VIEWS (views only) ===================

-- Discovery funnel by surface (hourly)
CREATE OR REPLACE VIEW DISCOVERY_FUNNEL_BY_SURFACE_T AS
WITH base AS (
  SELECT
    DATE_TRUNC('hour', ti.occurred_at) AS hr,
    ti.row_id, ti.position,
    ti.clicked, ti.conversion_flag, ti.conversion_latency_sec,
    se.surface,
    ti.user_id, ti.session_id, ti.occurred_at
  FROM DEMO_SILVER.TILE_IMPRESSION ti
  LEFT JOIN DEMO_SILVER.BROWSE_EVENT se
    ON se.session_id = ti.session_id
   AND se.user_id    = ti.user_id
   AND se.occurred_at BETWEEN DATEADD(minute,-2,ti.occurred_at) AND DATEADD(minute,2,ti.occurred_at)
),
agg AS (
  SELECT
    hr, COALESCE(surface,'unknown') AS surface, row_id, position,
    COUNT(*) AS impressions,
    SUM(IFF(clicked,1,0)) AS clicks,
    SUM(IFF(conversion_flag,1,0)) AS conversions,
    MEDIAN(NULLIF(conversion_latency_sec,0)) AS median_time_to_play_sec
  FROM base
  GROUP BY hr, surface, row_id, position
)
SELECT
  hr, surface, row_id, position, impressions, clicks, conversions,
  ROUND(clicks / NULLIF(impressions,0),4)      AS ctr,
  ROUND(conversions / NULLIF(clicks,0),4)      AS cvr_click_to_play,
  median_time_to_play_sec
FROM agg;

-- Exposure decay (by exposure_seq)
CREATE OR REPLACE VIEW EXPOSURE_DECAY_T AS
WITH exp AS (
  SELECT
    user_id, content_id, occurred_at, clicked, conversion_flag,
    ROW_NUMBER() OVER (PARTITION BY user_id, content_id ORDER BY occurred_at) AS exposure_seq
  FROM DEMO_SILVER.TILE_IMPRESSION
),
agg AS (
  SELECT
    exposure_seq,
    COUNT(*) AS impressions,
    SUM(IFF(clicked,1,0)) AS clicks,
    SUM(IFF(conversion_flag,1,0)) AS conversions
  FROM exp
  GROUP BY exposure_seq
)
SELECT
  exposure_seq, impressions, clicks, conversions,
  ROUND(clicks / NULLIF(impressions,0),4)       AS ctr,
  ROUND(conversions / NULLIF(impressions,0),4)  AS conv_rate
FROM agg
ORDER BY exposure_seq;

-- Notification → play within 24h
CREATE OR REPLACE VIEW NOTIFICATION_TO_PLAY_T AS
WITH notif AS (
  SELECT user_id, content_targeted_id, occurred_at AS sent_at, opened_ts, cta_clicked
  FROM DEMO_SILVER.NOTIFICATION_EVENT
),
plays AS (
  SELECT user_id, content_id, occurred_at AS play_at
  FROM DEMO_SILVER.ENGAGEMENT_EVENT
  WHERE action='play'
),
joined AS (
  SELECT
    n.user_id, n.sent_at, n.opened_ts, n.cta_clicked,
    p.play_at,
    DATEDIFF('minute', n.sent_at, p.play_at) AS minutes_to_play
  FROM notif n
  LEFT JOIN plays p
    ON p.user_id = n.user_id
   AND p.play_at BETWEEN n.sent_at AND DATEADD(hour,24,n.sent_at)
)
SELECT
  TO_DATE(sent_at) AS sent_date,
  COUNT(*) AS notif_sent,
  SUM(IFF(opened_ts IS NOT NULL,1,0)) AS notif_opened,
  SUM(IFF(cta_clicked,1,0))           AS notif_cta_clicked,
  SUM(IFF(play_at IS NOT NULL,1,0))   AS plays_within_24h,
  MEDIAN(IFF(play_at IS NOT NULL, minutes_to_play, NULL)) AS median_minutes_to_play,
  ROUND(SUM(IFF(opened_ts IS NOT NULL,1,0))/NULLIF(COUNT(*),0),4) AS open_rate,
  ROUND(SUM(IFF(cta_clicked,1,0))/NULLIF(COUNT(*),0),4)           AS cta_rate,
  ROUND(SUM(IFF(play_at IS NOT NULL,1,0))/NULLIF(COUNT(*),0),4)   AS play_rate_from_sent,
  ROUND(SUM(IFF(play_at IS NOT NULL,1,0))/NULLIF(SUM(IFF(cta_clicked,1,0)),0),4) AS play_rate_from_cta
FROM joined
GROUP BY TO_DATE(sent_at)
ORDER BY sent_date;

-- Friction heatmap (device × country × day)
CREATE OR REPLACE VIEW FRICTION_HEATMAP_T AS
WITH s AS (
  SELECT
    ss.user_id, ss.content_id, ss.device_model, ss.country, ss.occurred_at,
    ss.join_time_ms, ss.rebuffer_sec, ss.rebuffer_count
  FROM DEMO_SILVER.STREAM_SESSION ss
),
m AS (
  SELECT
    MIN(join_time_ms)   AS min_join,    MAX(join_time_ms)   AS max_join,
    MIN(rebuffer_sec)   AS min_rb_sec,  MAX(rebuffer_sec)   AS max_rb_sec,
    MIN(rebuffer_count) AS min_rb_cnt,  MAX(rebuffer_count) AS max_rb_cnt
  FROM s WHERE join_time_ms IS NOT NULL
)
SELECT
  DATE_TRUNC('day', s.occurred_at)           AS "DAY",
  COALESCE(s.device_model,'unknown')         AS device_model,
  COALESCE(s.country,'unknown')              AS country,
  AVG(
      0.5*CASE WHEN m.max_join  > m.min_join   THEN (s.join_time_ms   - m.min_join)  / NULLIF(m.max_join  - m.min_join ,0) ELSE 0 END
    + 1.0*CASE WHEN m.max_rb_sec> m.min_rb_sec THEN (s.rebuffer_sec   - m.min_rb_sec)/ NULLIF(m.max_rb_sec- m.min_rb_sec,0) ELSE 0 END
    + 0.5*CASE WHEN m.max_rb_cnt> m.min_rb_cnt THEN (s.rebuffer_count - m.min_rb_cnt)/ NULLIF(m.max_rb_cnt- m.min_rb_cnt,0) ELSE 0 END
  ) AS avg_friction_score,
  COUNT(*) AS sessions
FROM s, m
GROUP BY "DAY", device_model, country
ORDER BY "DAY", device_model, country;

-- Play source mix (daily share)
CREATE OR REPLACE VIEW PLAY_SOURCE_MIX_T AS
WITH plays AS (
  SELECT DATE_TRUNC('day', occurred_at) AS "DAY", COALESCE(play_source,'unknown') AS play_source
  FROM DEMO_SILVER.ENGAGEMENT_EVENT
  WHERE action='play' OR funnel_stage='conversion'
)
SELECT
  "DAY", play_source, COUNT(*) AS plays,
  ROUND(100.0*COUNT(*)/SUM(COUNT(*)) OVER (PARTITION BY "DAY"),2) AS pct_share
FROM plays
GROUP BY "DAY", play_source
ORDER BY "DAY", pct_share DESC;

-- 1) Micro Intent (dwell/preview + CTR/CVR)
CREATE OR REPLACE VIEW MICRO_INTENT_T AS
WITH base AS (
  SELECT
    DATE_TRUNC('day', ti.occurred_at)                  AS day_date,
    COALESCE(be.surface,'unknown')                      AS surface,
    ti.row_id, ti.position,
    ti.dwell_ms, ti.preview_watch_ms,
    ti.clicked, ti.conversion_flag
  FROM DEMO_SILVER.TILE_IMPRESSION ti
  LEFT JOIN DEMO_SILVER.BROWSE_EVENT be
    ON be.session_id = ti.session_id
   AND be.user_id    = ti.user_id
   AND be.occurred_at BETWEEN DATEADD(minute,-2,ti.occurred_at) AND DATEADD(minute,2,ti.occurred_at)
)
SELECT
  day_date, surface, row_id, position,
  COUNT(*)                                      AS impressions,
  AVG(NULLIF(dwell_ms,0))                       AS avg_dwell_ms,
  AVG(NULLIF(preview_watch_ms,0))               AS avg_preview_watch_ms,
  SUM(IFF(clicked,1,0))                         AS clicks,
  ROUND(SUM(IFF(clicked,1,0)) / NULLIF(COUNT(*),0), 4) AS ctr,
  SUM(IFF(conversion_flag,1,0))                 AS conversions,
  ROUND(SUM(IFF(conversion_flag,1,0)) / NULLIF(SUM(IFF(clicked,1,0)),0), 4) AS cvr_click_to_play
FROM base
GROUP BY day_date, surface, row_id, position;

-- 2) Friction-adjusted CVR
CREATE OR REPLACE VIEW FRICTION_ADJUSTED_CVR_T AS
WITH ti AS (
  SELECT
    ti.event_id,
    DATE_TRUNC('day', ti.occurred_at)           AS day_date,
    COALESCE(be.surface,'unknown')               AS surface,
    ti.row_id, ti.position,
    ti.clicked, ti.conversion_flag,
    ss.device_model, ss.country,
    ss.join_time_ms, ss.rebuffer_sec, ss.rebuffer_count
  FROM DEMO_SILVER.TILE_IMPRESSION ti
  LEFT JOIN DEMO_SILVER.STREAM_SESSION ss
    ON ss.session_id = ti.session_id
   AND ss.user_id    = ti.user_id
   AND ss.occurred_at BETWEEN DATEADD(minute,-5,ti.occurred_at) AND DATEADD(minute,5,ti.occurred_at)
  LEFT JOIN DEMO_SILVER.BROWSE_EVENT be
    ON be.session_id = ti.session_id
   AND be.user_id    = ti.user_id
   AND be.occurred_at BETWEEN DATEADD(minute,-2,ti.occurred_at) AND DATEADD(minute,2,ti.occurred_at)
),
bounds AS (
  SELECT
    MIN(join_time_ms)   AS min_join,    MAX(join_time_ms)   AS max_join,
    MIN(rebuffer_sec)   AS min_rb_sec,  MAX(rebuffer_sec)   AS max_rb_sec,
    MIN(rebuffer_count) AS min_rb_cnt,  MAX(rebuffer_count) AS max_rb_cnt
  FROM ti
  WHERE join_time_ms IS NOT NULL
),
scored AS (
  SELECT
    day_date, surface, row_id, position, clicked, conversion_flag,
    (  0.5 * IFF(b.max_join   > b.min_join,   (COALESCE(join_time_ms,0)-b.min_join)   / NULLIF(b.max_join-b.min_join,0), 0)
     + 1.0 * IFF(b.max_rb_sec > b.min_rb_sec, (COALESCE(rebuffer_sec,0)-b.min_rb_sec) / NULLIF(b.max_rb_sec-b.min_rb_sec,0), 0)
     + 0.5 * IFF(b.max_rb_cnt > b.min_rb_cnt, (COALESCE(rebuffer_count,0)-b.min_rb_cnt)/NULLIF(b.max_rb_cnt-b.min_rb_cnt,0), 0)
    ) AS friction_score
  FROM ti, bounds b
)
SELECT
  day_date, surface, row_id, position,
  COUNT(*)                                                     AS impressions,
  SUM(IFF(clicked,1,0))                                        AS clicks,
  SUM(IFF(conversion_flag,1,0))                                AS conversions,
  ROUND(SUM(IFF(clicked,1,0)) / NULLIF(COUNT(*),0), 4)         AS ctr,
  ROUND(SUM(IFF(conversion_flag,1,0)) / NULLIF(SUM(IFF(clicked,1,0)),0), 4) AS cvr,
  ROUND(
    SUM(IFF(conversion_flag, (1 - LEAST(GREATEST(friction_score,0),1)), 0))
    / NULLIF(SUM(IFF(clicked,1,0)),0)
  , 4) AS cvr_adj_friction,
  AVG(friction_score)                                          AS avg_friction_score
FROM scored
GROUP BY day_date, surface, row_id, position;

-- 3) Time-to-Value (touch → first play, same-day)
CREATE OR REPLACE VIEW TIME_TO_VALUE_T AS
WITH touches AS (
  SELECT user_id, DATE_TRUNC('day', occurred_at) AS day_date, MIN(occurred_at) AS first_touch_at
  FROM (
    SELECT user_id, occurred_at FROM DEMO_SILVER.BROWSE_EVENT
    UNION ALL SELECT user_id, occurred_at FROM DEMO_SILVER.SEARCH_EVENT
    UNION ALL SELECT user_id, occurred_at FROM DEMO_SILVER.TILE_IMPRESSION
    UNION ALL SELECT user_id, occurred_at FROM DEMO_SILVER.NOTIFICATION_EVENT
  )
  GROUP BY user_id, day_date
),
plays AS (
  SELECT user_id, DATE_TRUNC('day', occurred_at) AS day_date, MIN(occurred_at) AS first_play_at
  FROM DEMO_SILVER.ENGAGEMENT_EVENT
  WHERE action = 'play' OR funnel_stage = 'conversion'
  GROUP BY user_id, day_date
)
SELECT
  t.day_date,
  COUNT(*)                                             AS touched_users,
  COUNT(p.user_id)                                     AS users_with_play,
  ROUND(AVG(IFF(p.first_play_at IS NOT NULL, DATEDIFF('minute', t.first_touch_at, p.first_play_at), NULL)), 2) AS avg_minutes_to_value,
  ROUND(COUNT(p.user_id) / NULLIF(COUNT(*),0), 4)      AS same_day_touch_to_play_rate
FROM touches t
LEFT JOIN plays p
  ON p.user_id = t.user_id AND p.day_date = t.day_date
GROUP BY t.day_date
ORDER BY t.day_date;

-- 4) Slot Quality (CTR, CVR, micro-intent)
CREATE OR REPLACE VIEW SLOT_QUALITY_T AS
WITH m AS (
  SELECT surface, row_id, position,
         AVG(NULLIF(avg_dwell_ms,0))         AS avg_dwell_ms,
         AVG(NULLIF(avg_preview_watch_ms,0)) AS avg_preview_watch_ms
  FROM MICRO_INTENT_T
  GROUP BY surface, row_id, position
),
tile_stats AS (
  SELECT
    COALESCE(be.surface,'unknown')  AS surface,
    ti.row_id, ti.position,
    SUM(IFF(ti.clicked,1,0))        AS clicks,
    SUM(IFF(ti.conversion_flag,1,0)) AS conversions,
    COUNT(*)                         AS impressions
  FROM DEMO_SILVER.TILE_IMPRESSION ti
  LEFT JOIN DEMO_SILVER.BROWSE_EVENT be
    ON be.session_id = ti.session_id
   AND be.user_id    = ti.user_id
   AND be.occurred_at BETWEEN DATEADD(minute,-2,ti.occurred_at) AND DATEADD(minute,2,ti.occurred_at)
  GROUP BY surface, ti.row_id, ti.position
),
z AS (
  SELECT
    ts.surface, ts.row_id, ts.position,
    ROUND(ts.clicks / NULLIF(ts.impressions,0), 4) AS ctr,
    ROUND(ts.conversions / NULLIF(ts.clicks,0), 4) AS cvr,
    COALESCE(m.avg_dwell_ms,0) AS avg_dwell_ms,
    COALESCE(m.avg_preview_watch_ms,0) AS avg_preview_ms
  FROM tile_stats ts
  LEFT JOIN m
    ON m.surface=ts.surface AND m.row_id=ts.row_id AND m.position=ts.position
),
bnds AS (
  SELECT
    MIN(ctr) AS min_ctr, MAX(ctr) AS max_ctr,
    MIN(cvr) AS min_cvr, MAX(cvr) AS max_cvr,
    MIN(avg_dwell_ms) AS min_dwell, MAX(avg_dwell_ms) AS max_dwell,
    MIN(avg_preview_ms) AS min_prev, MAX(avg_preview_ms) AS max_prev
  FROM z
)
SELECT
  z.surface, z.row_id, z.position,
  z.ctr, z.cvr, z.avg_dwell_ms, z.avg_preview_ms,
  ROUND(
      0.40 * IFF(b.max_ctr  > b.min_ctr , (z.ctr          - b.min_ctr )/NULLIF(b.max_ctr -b.min_ctr ,0), 0)
    + 0.40 * IFF(b.max_cvr  > b.min_cvr , (z.cvr          - b.min_cvr )/NULLIF(b.max_cvr -b.min_cvr ,0), 0)
    + 0.10 * IFF(b.max_dwell> b.min_dwell, (z.avg_dwell_ms - b.min_dwell)/NULLIF(b.max_dwell-b.min_dwell,0), 0)
    + 0.10 * IFF(b.max_prev > b.min_prev , (z.avg_preview_ms- b.min_prev )/NULLIF(b.max_prev -b.min_prev ,0), 0)
  , 4) AS slot_quality_score,
  DENSE_RANK() OVER (ORDER BY
      0.40 * IFF(b.max_ctr  > b.min_ctr , (z.ctr          - b.min_ctr )/NULLIF(b.max_ctr -b.min_ctr ,0), 0)
    + 0.40 * IFF(b.max_cvr  > b.min_cvr , (z.cvr          - b.min_cvr )/NULLIF(b.max_cvr -b.min_cvr ,0), 0)
    + 0.10 * IFF(b.max_dwell> b.min_dwell, (z.avg_dwell_ms - b.min_dwell)/NULLIF(b.max_dwell-b.min_dwell,0), 0)
    + 0.10 * IFF(b.max_prev > b.min_prev , (z.avg_preview_ms- b.min_prev )/NULLIF(b.max_prev -b.min_prev ,0), 0)
  DESC) AS quality_rank
FROM z, bnds b
ORDER BY slot_quality_score DESC;

-- 5) Renewal cohort (first-purchase cohort, monthly curve)
CREATE OR REPLACE VIEW RENEWAL_COHORT_T AS
WITH first_sub AS (
  SELECT user_id,
         DATE_TRUNC('month', MIN(occurred_at)) AS cohort_month
  FROM DEMO_SILVER.COMMERCE_EVENT
  WHERE flow_step IN ('subscribe','purchase','trial_start')
  GROUP BY user_id
),
renew AS (
  SELECT
    ce.user_id,
    DATE_TRUNC('month', ce.occurred_at) AS event_month,
    MAX(IFF(ce.renewal_flag, 1, 0))     AS any_renewal
  FROM DEMO_SILVER.COMMERCE_EVENT ce
  GROUP BY ce.user_id, DATE_TRUNC('month', ce.occurred_at)
),
joined AS (
  SELECT
    fs.cohort_month,
    r.event_month,
    DATEDIFF('month', fs.cohort_month, r.event_month) AS month_num,
    r.any_renewal
  FROM first_sub fs
  LEFT JOIN renew r
    ON r.user_id = fs.user_id
  WHERE r.event_month >= fs.cohort_month
)
SELECT
  cohort_month,
  month_num,
  COUNT(*)                                      AS cohort_size_or_obs,
  SUM(any_renewal)                              AS renewals,
  ROUND(SUM(any_renewal) / NULLIF(COUNT(*),0),4) AS renewal_rate
FROM joined
GROUP BY cohort_month, month_num
ORDER BY cohort_month, month_num;

-- ================ Streamlit-ready slim views ================
CREATE OR REPLACE VIEW SL_FACT_SESSION AS
SELECT
  fs.*, du.country, dd.device_model, dl.location_id, dt.occurred_hour AS hour_bucket
FROM FACT_SESSION_T fs
LEFT JOIN DIM_USER_T     du ON du.user_sk     = fs.user_sk
LEFT JOIN DIM_DEVICE_T   dd ON dd.device_sk   = fs.device_sk
LEFT JOIN DIM_LOCATION_T dl ON dl.location_sk = fs.location_sk
LEFT JOIN DIM_TIME_T     dt ON dt.time_sk     = fs.time_sk;

CREATE OR REPLACE VIEW SL_FACT_ENGAGEMENT AS
SELECT
  fe.*, du.country, dt.occurred_hour AS hour_bucket
FROM FACT_ENGAGEMENT_T fe
LEFT JOIN DIM_USER_T du ON du.user_sk = fe.user_sk
LEFT JOIN DIM_TIME_T dt ON dt.time_sk = fe.time_sk;

CREATE OR REPLACE VIEW SL_FACT_TILE_IMPRESSION AS
SELECT
  ti.*, du.country, dt.occurred_hour AS hour_bucket
FROM FACT_TILE_IMPRESSION_T ti
LEFT JOIN DIM_USER_T du ON du.user_sk = ti.user_sk
LEFT JOIN DIM_TIME_T dt ON dt.time_sk = ti.time_sk;

CREATE OR REPLACE VIEW SL_FACT_SEARCH AS
SELECT s.*, du.country, dt.occurred_hour AS hour_bucket
FROM FACT_SEARCH_T s
LEFT JOIN DIM_USER_T du ON du.user_sk = s.user_sk
LEFT JOIN DIM_TIME_T dt ON dt.time_sk = s.time_sk;

CREATE OR REPLACE VIEW SL_FACT_NOTIFICATION AS
SELECT n.*, du.country, dt.occurred_hour AS hour_bucket
FROM FACT_NOTIFICATION_T n
LEFT JOIN DIM_USER_T du ON du.user_sk = n.user_sk
LEFT JOIN DIM_TIME_T dt ON dt.time_sk = n.time_sk;

CREATE OR REPLACE VIEW SL_FACT_COMMERCE AS
SELECT c.*, du.country, dt.occurred_hour AS hour_bucket
FROM FACT_COMMERCE_T c
LEFT JOIN DIM_USER_T du ON du.user_sk = c.user_sk
LEFT JOIN DIM_TIME_T dt ON dt.time_sk = c.time_sk;

-- Slim aliases for KPIs
CREATE OR REPLACE VIEW SL_DISCOVERY_FUNNEL_BY_SURFACE AS SELECT * FROM DISCOVERY_FUNNEL_BY_SURFACE_T;
CREATE OR REPLACE VIEW SL_EXPOSURE_DECAY              AS SELECT * FROM EXPOSURE_DECAY_T;
CREATE OR REPLACE VIEW SL_NOTIFICATION_TO_PLAY       AS SELECT * FROM NOTIFICATION_TO_PLAY_T;
CREATE OR REPLACE VIEW SL_FRICTION_HEATMAP           AS SELECT * FROM FRICTION_HEATMAP_T;
CREATE OR REPLACE VIEW SL_PLAY_SOURCE_MIX            AS SELECT * FROM PLAY_SOURCE_MIX_T;

CREATE OR REPLACE VIEW SL_MICRO_INTENT              AS SELECT * FROM MICRO_INTENT_T;
CREATE OR REPLACE VIEW SL_FRICTION_ADJUSTED_CVR     AS SELECT * FROM FRICTION_ADJUSTED_CVR_T;
CREATE OR REPLACE VIEW SL_TIME_TO_VALUE             AS SELECT * FROM TIME_TO_VALUE_T;
CREATE OR REPLACE VIEW SL_SLOT_QUALITY              AS SELECT * FROM SLOT_QUALITY_T;
CREATE OR REPLACE VIEW SL_RENEWAL_COHORT            AS SELECT * FROM RENEWAL_COHORT_T;