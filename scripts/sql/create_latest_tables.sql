-- ============================================================
-- ads_latest: dataset chứa bản ghi mới nhất từ ads_raw
-- Dùng cho view vw_insights_looker, tránh duplicate khi
-- daily chạy nhiều lần trong cùng ngày.
-- ============================================================

CREATE SCHEMA IF NOT EXISTS `ads_latest`;

-- Bảng account mới nhất
CREATE TABLE IF NOT EXISTS `ads_latest.latest_flatform_account` (
  id STRING NOT NULL,
  name STRING,
  account_status INT64,
  currency STRING,
  timezone_name STRING,
  created_time TIMESTAMP,
  updated_time TIMESTAMP,
  spend_cap FLOAT64,
  amount_spent FLOAT64,
  flatform STRING,
  fetched_at TIMESTAMP NOT NULL
);

-- Bảng campaign mới nhất (bao gồm branch, service, type_campaign)
CREATE TABLE IF NOT EXISTS `ads_latest.latest_flatform_campaign` (
  id STRING NOT NULL,
  account_id STRING,
  name STRING,
  objective STRING,
  status STRING,
  configured_status STRING,
  effective_status STRING,
  buying_type STRING,
  daily_budget FLOAT64,
  lifetime_budget FLOAT64,
  start_time TIMESTAMP,
  stop_time TIMESTAMP,
  created_time TIMESTAMP,
  updated_time TIMESTAMP,
  timezone_name STRING,
  branch STRING,
  service STRING,
  type_campaign STRING,
  flatform STRING,
  fetched_at TIMESTAMP NOT NULL
);

-- Bảng insights mới nhất theo ngày
CREATE TABLE IF NOT EXISTS `ads_latest.latest_flatform_insights` (
  account_id STRING,
  campaign_id STRING,
  spend FLOAT64,
  date_start DATE,
  date_stop DATE,
  account_timezone STRING,
  flatform STRING NOT NULL,
  fetched_at TIMESTAMP NOT NULL
);
