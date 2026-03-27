CREATE SCHEMA IF NOT EXISTS `ads_raw`;

CREATE TABLE IF NOT EXISTS `ads_raw.raw_flatform_account` (
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

CREATE TABLE IF NOT EXISTS `ads_raw.raw_flatform_campaign` (
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

CREATE TABLE IF NOT EXISTS `ads_raw.raw_flatform_insights` (
  account_id        STRING,
  campaign_id       STRING,
  spend            FLOAT64,
  date_start       DATE,
  date_stop        DATE,
  account_timezone STRING,
  flatform         STRING NOT NULL,
  fetched_at       TIMESTAMP NOT NULL
);

-- Thêm column cho bảng đã tồn tại
ALTER TABLE `ads_raw.raw_flatform_campaign`
ADD COLUMN IF NOT EXISTS timezone_name STRING;

ALTER TABLE `ads_raw.raw_flatform_insights`
ADD COLUMN IF NOT EXISTS account_timezone STRING;

-- Thêm column branch, service, type_campaign cho campaign
ALTER TABLE `ads_raw.raw_flatform_campaign`
ADD COLUMN IF NOT EXISTS branch STRING,
ADD COLUMN IF NOT EXISTS service STRING,
ADD COLUMN IF NOT EXISTS type_campaign STRING;
