-- ============================================================
-- refresh_latest_tables.sql
-- MERGE từ ads_raw -> ads_latest để giữ bản ghi mới nhất.
-- Chạy SAU mỗi lần ingest raw data (daily / backfill).
-- ============================================================

-- ── 1. latest_flatform_account ──────────────────────────────────────────────
-- Business key: (flatform, id)
-- Tie-breaker: fetched_at DESC, updated_time DESC, created_time DESC

MERGE `ads_latest.latest_flatform_account` T
USING (
  SELECT *
  FROM `ads_raw.raw_flatform_account`
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY flatform, id
    ORDER BY fetched_at DESC, updated_time DESC, created_time DESC
  ) = 1
) S
ON T.flatform = S.flatform AND T.id = S.id
WHEN MATCHED AND S.fetched_at > T.fetched_at THEN UPDATE SET
  name = S.name,
  account_status = S.account_status,
  currency = S.currency,
  timezone_name = S.timezone_name,
  created_time = S.created_time,
  updated_time = S.updated_time,
  spend_cap = S.spend_cap,
  amount_spent = S.amount_spent,
  fetched_at = S.fetched_at
WHEN NOT MATCHED THEN
  INSERT ROW;


-- ── 2. latest_flatform_campaign ─────────────────────────────────────────────
-- Business key: (flatform, account_id, id)
-- Tie-breaker: fetched_at DESC, updated_time DESC, created_time DESC

MERGE `ads_latest.latest_flatform_campaign` T
USING (
  SELECT *
  FROM `ads_raw.raw_flatform_campaign`
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY flatform, account_id, id
    ORDER BY fetched_at DESC, updated_time DESC, created_time DESC
  ) = 1
) S
ON T.flatform = S.flatform AND T.account_id = S.account_id AND T.id = S.id
WHEN MATCHED AND S.fetched_at > T.fetched_at THEN UPDATE SET
  name = S.name,
  objective = S.objective,
  status = S.status,
  configured_status = S.configured_status,
  effective_status = S.effective_status,
  buying_type = S.buying_type,
  daily_budget = S.daily_budget,
  lifetime_budget = S.lifetime_budget,
  start_time = S.start_time,
  stop_time = S.stop_time,
  created_time = S.created_time,
  updated_time = S.updated_time,
  timezone_name = S.timezone_name,
  branch = S.branch,
  service = S.service,
  type_campaign = S.type_campaign,
  fetched_at = S.fetched_at
WHEN NOT MATCHED THEN
  INSERT ROW;


-- ── 3. latest_flatform_insights ─────────────────────────────────────────────
-- Business key: (flatform, account_id, campaign_id, date_start)
-- Tie-breaker: fetched_at DESC

MERGE `ads_latest.latest_flatform_insights` T
USING (
  SELECT *
  FROM `ads_raw.raw_flatform_insights`
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY flatform, account_id, campaign_id, date_start
    ORDER BY fetched_at DESC
  ) = 1
) S
ON T.flatform = S.flatform
AND T.account_id = S.account_id
AND T.campaign_id = S.campaign_id
AND T.date_start = S.date_start
WHEN MATCHED AND S.fetched_at > T.fetched_at THEN UPDATE SET
  spend = S.spend,
  date_stop = S.date_stop,
  account_timezone = S.account_timezone,
  fetched_at = S.fetched_at
WHEN NOT MATCHED THEN
  INSERT ROW;
