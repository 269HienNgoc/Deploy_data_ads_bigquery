package service

import (
	"context"
	"fmt"

	"github.com/robfig/cron/v3"

	"deploy_data_bigquery/internal/config"
	"deploy_data_bigquery/internal/logger"
)

// Scheduler registers and runs cron jobs.
type Scheduler struct {
	cfg      *config.Config
	pipeline *Pipeline
	cron     *cron.Cron
}

func NewScheduler(cfg *config.Config, pipeline *Pipeline) *Scheduler {
	return &Scheduler{
		cfg:      cfg,
		pipeline: pipeline,
		cron:     cron.New(cron.WithSeconds()),
	}
}

func (s *Scheduler) RegisterJobs(ctx context.Context) error {
	if s.cfg.AppMode == "backfill" {
		_, err := s.cron.AddFunc("0 0 1 * * *", func() {
			if err := s.pipeline.RunBackfill(ctx); err != nil {
				logger.GetLogger().Error("SCHEDULER: backfill job failed")
			}
		})
		if err != nil {
			return fmt.Errorf("register backfill job: %w", err)
		}
		return nil
	}

	// Convert 5-field cron from env to 6-field cron by prepending seconds=0.
	spec := "0 " + s.cfg.CronSchedule
	_, err := s.cron.AddFunc(spec, func() {
		if err := s.pipeline.RunDaily(ctx); err != nil {
			logger.GetLogger().Error("SCHEDULER: daily job failed")
		}
	})
	if err != nil {
		return fmt.Errorf("register daily job: %w", err)
	}
	return nil
}

func (s *Scheduler) Start() {
	logger.GetLogger().Info("SCHEDULER: started")
	s.cron.Start()
}

func (s *Scheduler) Stop(ctx context.Context) {
	stopCtx := s.cron.Stop()
	select {
	case <-ctx.Done():
		return
	case <-stopCtx.Done():
		logger.GetLogger().Info("SCHEDULER: stopped")
	}
}
