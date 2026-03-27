CREATE SCHEMA IF NOT EXISTS `ads_mart`;

CREATE TABLE IF NOT EXISTS `ads_mart.mart_platform_insights` (
  date          DATE        NOT NULL,
  date_start    DATE,
  date_stop     DATE,
  flatform      STRING,
  account_id    STRING,
  account_name  STRING,
  campaign_id   STRING,
  campaign_name STRING,
  status        STRING,
  daily_budget  FLOAT64,
  spend         FLOAT64,
  branch        STRING,
  service       STRING,
  type_campaign STRING,
  fetched_at    TIMESTAMP   NOT NULL
)
PARTITION BY date
CLUSTER BY flatform, account_id;

-- VIEW dành cho Looker Studio: JOIN 3 bảng latest (ads_latest)
-- Lấy bản ghi mới nhất theo fetched_at, không duplicate
CREATE OR REPLACE VIEW `ads_mart.vw_insights_looker` AS
SELECT
  i.date_start    AS date,
  i.date_start,
  i.date_stop,
  i.flatform,
  i.account_id,
  a.name          AS account_name,
  i.campaign_id,
  c.name          AS campaign_name,
  c.status,
  c.daily_budget,
  c.branch,
  c.service,
  c.type_campaign,
  i.spend,
  i.fetched_at
FROM `ads_latest.latest_flatform_insights` AS i
LEFT JOIN `ads_latest.latest_flatform_account`  AS a
  ON i.flatform = a.flatform
 AND i.account_id = a.id
LEFT JOIN `ads_latest.latest_flatform_campaign` AS c
  ON i.flatform = c.flatform
 AND i.account_id = c.account_id
 AND i.campaign_id = c.id;
