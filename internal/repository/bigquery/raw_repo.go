package bigquery

import (
	"context"
	"fmt"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/option"
	"go.uber.org/zap"

	"deploy_data_bigquery/internal/config"
	"deploy_data_bigquery/internal/dlq"
	"deploy_data_bigquery/internal/logger"
	"deploy_data_bigquery/internal/models"
	"deploy_data_bigquery/internal/observability"
)

const (
	rawAccountTable  = "raw_flatform_account"
	rawCampaignTable = "raw_flatform_campaign"
	rawInsightTable  = "raw_flatform_insights"
)

// RawRepository stores account/campaign raw data.
type RawRepository struct {
	client     *bigquery.Client
	projectID  string
	dataset    string
	batchSize  int
	maxRetries int
	retryDelay time.Duration
	dryRun     bool
	metrics    *observability.Metrics
}

func NewRawRepository(ctx context.Context, cfg *config.Config) (*RawRepository, error) {
	if cfg.GoogleApplicationCredentials == "" {
		return nil, fmt.Errorf("new bigquery client: GOOGLE_APPLICATION_CREDENTIALS is empty")
	}

	client, err := bigquery.NewClient(ctx, cfg.BQProjectID, option.WithCredentialsFile(cfg.GoogleApplicationCredentials))
	if err != nil {
		return nil, fmt.Errorf("new bigquery client: %w", err)
	}

	batchSize := cfg.BatchSize
	if batchSize <= 0 {
		batchSize = 500
	}

	maxRetries := max(cfg.MaxRetries, 0)
	retryDelay := cfg.RetryBaseDelay
	if retryDelay <= 0 {
		retryDelay = time.Second
	}

	return &RawRepository{
		client:     client,
		projectID:  cfg.BQProjectID,
		dataset:    cfg.BQDatasetRaw,
		batchSize:  batchSize,
		maxRetries: maxRetries,
		retryDelay: retryDelay,
		dryRun:     cfg.DryRun,
	}, nil
}

func (r *RawRepository) Close() error {
	return r.client.Close()
}

func (r *RawRepository) SetMetrics(metrics *observability.Metrics) {
	r.metrics = metrics
}

func (r *RawRepository) InsertAccounts(ctx context.Context, accounts []models.Account) error {
	if len(accounts) == 0 {
		return nil
	}

	if r.dryRun {
		if r.metrics != nil {
			r.metrics.AddDryRunSkip(len(accounts))
		}
		logger.GetLogger().Info("BQ_RAW: dry-run skip insert accounts")
		return nil
	}

	inserter := r.client.Dataset(r.dataset).Table(rawAccountTable).Inserter()
	for start := 0; start < len(accounts); start += r.batchSize {
		end := start + r.batchSize
		if end > len(accounts) {
			end = len(accounts)
		}
		batch := accounts[start:end]

		var lastErr error
		delay := r.retryDelay
		for attempt := 0; attempt <= r.maxRetries; attempt++ {
			if err := inserter.Put(ctx, batch); err != nil {
				lastErr = err
			} else {
				lastErr = nil
				break
			}
			if attempt == r.maxRetries {
				break
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
			}
			delay *= 2
		}
		if lastErr != nil {
			_ = dlq.Write("raw_account_insert_failed", batch, lastErr)
			return fmt.Errorf("insert accounts batch [%d:%d]: %w", start, end, lastErr)
		}
	}

	if r.metrics != nil {
		r.metrics.AddRowsInsertedRaw(len(accounts))
	}
	logger.GetLogger().Info("BQ_RAW: inserted accounts")
	return nil
}

func (r *RawRepository) InsertCampaigns(ctx context.Context, campaigns []models.Campaign) error {
	if len(campaigns) == 0 {
		return nil
	}

	if r.dryRun {
		if r.metrics != nil {
			r.metrics.AddDryRunSkip(len(campaigns))
		}
		logger.GetLogger().Info("BQ_RAW: dry-run skip insert campaigns")
		return nil
	}

	inserter := r.client.Dataset(r.dataset).Table(rawCampaignTable).Inserter()
	for start := 0; start < len(campaigns); start += r.batchSize {
		end := start + r.batchSize
		if end > len(campaigns) {
			end = len(campaigns)
		}
		batch := campaigns[start:end]

		var lastErr error
		delay := r.retryDelay
		for attempt := 0; attempt <= r.maxRetries; attempt++ {
			if err := inserter.Put(ctx, batch); err != nil {
				lastErr = err
			} else {
				lastErr = nil
				break
			}
			if attempt == r.maxRetries {
				break
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
			}
			delay *= 2
		}
		if lastErr != nil {
			_ = dlq.Write("raw_campaign_insert_failed", batch, lastErr)
			return fmt.Errorf("insert campaigns batch [%d:%d]: %w", start, end, lastErr)
		}
	}

	if r.metrics != nil {
		r.metrics.AddRowsInsertedRaw(len(campaigns))
	}
	logger.GetLogger().Info("BQ_RAW: inserted campaigns")
	return nil
}

// EnsureTables checks all three raw tables exist by trying metadata retrieval.
func (r *RawRepository) EnsureTables(ctx context.Context) error {
	for _, table := range []string{rawAccountTable, rawCampaignTable, rawInsightTable} {
		if _, err := r.client.Dataset(r.dataset).Table(table).Metadata(ctx); err != nil {
			return fmt.Errorf("get table metadata %s.%s: %w", r.dataset, table, err)
		}
	}
	return nil
}

// InsertInsights inserts raw Facebook campaign insights into ads_raw.raw_flatform_insights.
// Each call appends rows (no upsert) — dedup handled downstream in ads_mart.
func (r *RawRepository) InsertInsights(ctx context.Context, insights []models.RawInsight) error {
	if len(insights) == 0 {
		return nil
	}

	if r.dryRun {
		if r.metrics != nil {
			r.metrics.AddDryRunSkip(len(insights))
		}
		logger.GetLogger().Info("BQ_RAW: dry-run skip insert insights")
		return nil
	}

	inserter := r.client.Dataset(r.dataset).Table(rawInsightTable).Inserter()
	for start := 0; start < len(insights); start += r.batchSize {
		end := start + r.batchSize
		if end > len(insights) {
			end = len(insights)
		}
		batch := insights[start:end]

		var lastErr error
		delay := r.retryDelay
		for attempt := 0; attempt <= r.maxRetries; attempt++ {
			if err := inserter.Put(ctx, batch); err != nil {
				lastErr = err
			} else {
				lastErr = nil
				break
			}
			if attempt == r.maxRetries {
				break
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
			}
			delay *= 2
		}
		if lastErr != nil {
			_ = dlq.Write("raw_insight_insert_failed", batch, lastErr)
			return fmt.Errorf("insert insights batch [%d:%d]: %w", start, end, lastErr)
		}
	}

	if r.metrics != nil {
		r.metrics.AddRowsInsertedRaw(len(insights))
	}
	logger.GetLogger().Info("BQ_RAW: inserted insights")
	return nil
}

// RefreshLatestTables runs MERGE statements to sync ads_latest from ads_raw.
// Executes after every RunDaily and RunBackfill to keep the serving layer up to date.
func (r *RawRepository) RefreshLatestTables(ctx context.Context) error {
	if r.dryRun {
		logger.GetLogger().Info("BQ_RAW: dry-run skip refresh latest tables")
		return nil
	}

	log := logger.GetLogger()

	// Run MERGE for accounts
	if err := r.runMerge(ctx, mergeLatestAccountSQL); err != nil {
		log.Warn("BQ_RAW: failed to refresh latest accounts", zap.Error(err))
	}

	// Run MERGE for campaigns
	if err := r.runMerge(ctx, mergeLatestCampaignSQL); err != nil {
		log.Warn("BQ_RAW: failed to refresh latest campaigns", zap.Error(err))
	}

	// Run MERGE for insights
	if err := r.runMerge(ctx, mergeLatestInsightSQL); err != nil {
		log.Warn("BQ_RAW: failed to refresh latest insights", zap.Error(err))
	}

	log.Info("BQ_RAW: refresh latest tables completed")
	return nil
}

func (r *RawRepository) runMerge(ctx context.Context, sql string) error {
	if sql == "" {
		return nil
	}
	// Replace placeholder with actual project ID
	sql = strings.ReplaceAll(sql, "PROJECT_ID_PLACEHOLDER", r.projectID)
	query := r.client.Query(sql)
	job, err := query.Run(ctx)
	if err != nil {
		return fmt.Errorf("run merge job: %w", err)
	}
	status, err := job.Wait(ctx)
	if err != nil {
		return fmt.Errorf("wait merge job: %w", err)
	}
	if status.Err() != nil {
		return fmt.Errorf("merge job error: %w", status.Err())
	}
	return nil
}

const (
	mergeLatestAccountSQL = `
MERGE ` + "`" + `PROJECT_ID_PLACEHOLDER.ads_latest.latest_flatform_account` + "`" + ` T
USING (
  SELECT *
  FROM ` + "`" + `PROJECT_ID_PLACEHOLDER.ads_raw.raw_flatform_account` + "`" + `
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
  INSERT ROW;`

	mergeLatestCampaignSQL = `
MERGE ` + "`" + `PROJECT_ID_PLACEHOLDER.ads_latest.latest_flatform_campaign` + "`" + ` T
USING (
  SELECT *
  FROM ` + "`" + `PROJECT_ID_PLACEHOLDER.ads_raw.raw_flatform_campaign` + "`" + `
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
  INSERT ROW;`

	mergeLatestInsightSQL = `
MERGE ` + "`" + `PROJECT_ID_PLACEHOLDER.ads_latest.latest_flatform_insights` + "`" + ` T
USING (
  SELECT *
  FROM ` + "`" + `PROJECT_ID_PLACEHOLDER.ads_raw.raw_flatform_insights` + "`" + `
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
  INSERT ROW;`
)
