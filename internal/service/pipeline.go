package service

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	"deploy_data_bigquery/internal/config"
	"deploy_data_bigquery/internal/logger"
	"deploy_data_bigquery/internal/models"
	"deploy_data_bigquery/internal/observability"
	"deploy_data_bigquery/internal/repository"
	facebooksvc "deploy_data_bigquery/internal/service/facebook"
	"deploy_data_bigquery/internal/worker"
)

// Pipeline orchestrates fetch -> transform -> load (accounts + campaigns + raw insights).
type Pipeline struct {
	cfg             *config.Config
	fetcher         *facebooksvc.Fetcher
	accountRepo     repository.AccountRepository
	campaignRepo    repository.CampaignRepository
	rawInsightRepo  repository.RawInsightRepository
	pool            *worker.Pool
	metrics         *observability.Metrics
}

func NewPipeline(
	cfg *config.Config,
	fetcher *facebooksvc.Fetcher,
	accountRepo repository.AccountRepository,
	campaignRepo repository.CampaignRepository,
	rawInsightRepo repository.RawInsightRepository,
	metrics *observability.Metrics,
) *Pipeline {
	if mw, ok := campaignRepo.(repository.MetricsAware); ok {
		mw.SetMetrics(metrics)
	}
	if mw, ok := accountRepo.(repository.MetricsAware); ok {
		mw.SetMetrics(metrics)
	}
	if mw, ok := rawInsightRepo.(repository.MetricsAware); ok {
		mw.SetMetrics(metrics)
	}

	return &Pipeline{
		cfg:             cfg,
		fetcher:         fetcher,
		accountRepo:     accountRepo,
		campaignRepo:    campaignRepo,
		rawInsightRepo:  rawInsightRepo,
		pool:            worker.NewPool(cfg.MaxWorkers),
		metrics:         metrics,
	}
}

// RunDaily fetches latest accounts + campaigns + raw insights.
func (p *Pipeline) RunDaily(ctx context.Context) error {
	log := logger.GetLogger()
	start := time.Now()
	if p.metrics != nil {
		p.metrics.MarkRunStart()
	}
	log.Info("PIPELINE: daily run started")

	accounts, err := p.fetcher.FetchAccounts(ctx)
	if err != nil {
		if p.metrics != nil {
			p.metrics.MarkRunFailed(time.Since(start))
		}
		return fmt.Errorf("fetch accounts: %w", err)
	}
	if p.metrics != nil {
		p.metrics.AddAccountsFetched(len(accounts))
	}
	if err := p.accountRepo.InsertAccounts(ctx, accounts); err != nil {
		if p.metrics != nil {
			p.metrics.MarkRunFailed(time.Since(start))
		}
		return fmt.Errorf("insert accounts: %w", err)
	}

	var (
		campaignsMu  sync.Mutex
		allCampaigns []models.Campaign
		insightsMu   sync.Mutex
		allInsights  []models.RawInsight
		jobs         []worker.Job
	)

	for _, acc := range accounts {
		account := acc
		jobs = append(jobs, func(jobCtx context.Context) error {
			campaigns, err := p.fetcher.FetchCampaigns(jobCtx, account.ID, account.TimezoneName)
			if err != nil {
				return fmt.Errorf("fetch campaigns account=%s: %w", account.ID, err)
			}
			campaignsMu.Lock()
			allCampaigns = append(allCampaigns, campaigns...)
			campaignsMu.Unlock()
			return nil
		})
	}

	// Fetch raw insights (today) for all accounts
	for _, acc := range accounts {
		account := acc
		jobs = append(jobs, func(jobCtx context.Context) error {
			insights, err := p.fetcher.FetchRawInsights(jobCtx, account.ID, account.TimezoneName, "today", models.TimeRange{})
			if err != nil {
				return fmt.Errorf("fetch raw insights account=%s: %w", account.ID, err)
			}
			insightsMu.Lock()
			allInsights = append(allInsights, insights...)
			insightsMu.Unlock()
			return nil
		})
	}

	if err := p.pool.Run(ctx, jobs); err != nil {
		return fmt.Errorf("worker pool daily: %w", err)
	}

	if p.metrics != nil {
		p.metrics.AddCampaignsFetched(len(allCampaigns))
		p.metrics.AddInsightsFetched(len(allInsights))
	}

	if err := p.campaignRepo.InsertCampaigns(ctx, allCampaigns); err != nil {
		if p.metrics != nil {
			p.metrics.MarkRunFailed(time.Since(start))
		}
		return fmt.Errorf("insert campaigns: %w", err)
	}

	if err := p.rawInsightRepo.InsertInsights(ctx, allInsights); err != nil {
		if p.metrics != nil {
			p.metrics.MarkRunFailed(time.Since(start))
		}
		return fmt.Errorf("insert raw insights: %w", err)
	}

	// Sync ads_latest after raw insert
	if err := p.rawInsightRepo.RefreshLatestTables(ctx); err != nil {
		log.Warn("PIPELINE: refresh latest tables failed", zap.Error(err))
		// continue — raw insert succeeded, latest can be retried next run
	}

	duration := time.Since(start)
	if p.metrics != nil {
		p.metrics.MarkRunSuccess(duration)
	}
	log.Info("PIPELINE: daily run completed",
		zap.Int("accounts", len(accounts)),
		zap.Int("campaigns", len(allCampaigns)),
		zap.Duration("duration", duration),
	)
	return nil
}

// RunBackfill fetches historical accounts, campaigns, and raw insights by day.
func (p *Pipeline) RunBackfill(ctx context.Context) error {
	log := logger.GetLogger()
	start := time.Now()
	if p.metrics != nil {
		p.metrics.MarkRunStart()
	}
	log.Info("PIPELINE: backfill run started", zap.String("since", p.cfg.BackfillSince))

	accounts, err := p.fetcher.FetchAccounts(ctx)
	if err != nil {
		if p.metrics != nil {
			p.metrics.MarkRunFailed(time.Since(start))
		}
		return fmt.Errorf("fetch accounts: %w", err)
	}
	if p.metrics != nil {
		p.metrics.AddAccountsFetched(len(accounts))
	}
	if err := p.accountRepo.InsertAccounts(ctx, accounts); err != nil {
		if p.metrics != nil {
			p.metrics.MarkRunFailed(time.Since(start))
		}
		return fmt.Errorf("insert accounts: %w", err)
	}

	days, err := facebooksvc.GenerateBackfillDays(p.cfg.BackfillSince)
	if err != nil {
		if p.metrics != nil {
			p.metrics.MarkRunFailed(time.Since(start))
		}
		return fmt.Errorf("generate backfill days: %w", err)
	}
	if len(days) == 0 {
		return fmt.Errorf("no backfill days generated from %s", p.cfg.BackfillSince)
	}

	// Fetch all campaigns once (same across all days)
	log.Info("PIPELINE: fetching campaigns for all accounts")
	var campaignsMu sync.Mutex
	allCampaigns := []models.Campaign{}
	var campaignJobs []worker.Job
	for _, acc := range accounts {
		account := acc
		campaignJobs = append(campaignJobs, func(jobCtx context.Context) error {
			campaigns, err := p.fetcher.FetchCampaigns(jobCtx, account.ID, account.TimezoneName)
			if err != nil {
				return fmt.Errorf("fetch campaigns account=%s: %w", account.ID, err)
			}
			campaignsMu.Lock()
			allCampaigns = append(allCampaigns, campaigns...)
			campaignsMu.Unlock()
			return nil
		})
	}
	if err := p.pool.Run(ctx, campaignJobs); err != nil {
		if p.metrics != nil {
			p.metrics.MarkRunFailed(time.Since(start))
		}
		return fmt.Errorf("worker pool backfill campaigns: %w", err)
	}
	if err := p.campaignRepo.InsertCampaigns(ctx, allCampaigns); err != nil {
		if p.metrics != nil {
			p.metrics.MarkRunFailed(time.Since(start))
		}
		return fmt.Errorf("insert backfill campaigns: %w", err)
	}
	if p.metrics != nil {
		p.metrics.AddCampaignsFetched(len(allCampaigns))
	}
	log.Info("PIPELINE: campaigns fetched", zap.Int("total", len(allCampaigns)))

	// Process each day: fetch insights + insert
	var totalInsights int
	for _, day := range days {
		log.Info("PIPELINE: processing day", zap.String("date", day))

		var insightsMu sync.Mutex
		allInsights := []models.RawInsight{}
		var insightJobs []worker.Job

		for _, acc := range accounts {
			account := acc
			insightJobs = append(insightJobs, func(jobCtx context.Context) error {
				tr := facebooksvc.DayTimeRange(day)
				insights, err := p.fetcher.FetchRawInsights(jobCtx, account.ID, account.TimezoneName, "", tr)
				if err != nil {
					// Retry once immediately
					insights, err = p.fetcher.FetchRawInsights(jobCtx, account.ID, account.TimezoneName, "", tr)
					if err != nil {
						log.Warn("PIPELINE: backfill day failed after retry",
							zap.String("date", day),
							zap.String("account_id", account.ID),
							zap.Error(err),
						)
						return nil // skip this account/day, don't fail the whole job
					}
				}
				insightsMu.Lock()
				allInsights = append(allInsights, insights...)
				insightsMu.Unlock()
				return nil
			})
		}

		if err := p.pool.Run(ctx, insightJobs); err != nil {
			if p.metrics != nil {
				p.metrics.MarkRunFailed(time.Since(start))
			}
			return fmt.Errorf("worker pool backfill day=%s: %w", day, err)
		}

		// Insert insights for this day
		if err := p.rawInsightRepo.InsertInsights(ctx, allInsights); err != nil {
			if p.metrics != nil {
				p.metrics.MarkRunFailed(time.Since(start))
			}
			return fmt.Errorf("insert backfill insights day=%s: %w", day, err)
		}

		// Sync ads_latest for this day (before moving to next day)
		if err := p.rawInsightRepo.RefreshLatestTables(ctx); err != nil {
			log.Warn("PIPELINE: refresh latest tables failed after day",
				zap.String("date", day),
				zap.Error(err),
			)
			// continue — raw insert succeeded
		}

		totalInsights += len(allInsights)
		if p.metrics != nil {
			p.metrics.AddInsightsFetched(len(allInsights))
		}

		log.Info("PIPELINE: day completed",
			zap.String("date", day),
			zap.Int("insights", len(allInsights)),
		)
	}

	duration := time.Since(start)
	if p.metrics != nil {
		p.metrics.MarkRunSuccess(duration)
	}
	log.Info("PIPELINE: backfill run completed",
		zap.Int("total_days", len(days)),
		zap.Int("total_insights", totalInsights),
		zap.Duration("duration", duration),
	)
	return nil
}
