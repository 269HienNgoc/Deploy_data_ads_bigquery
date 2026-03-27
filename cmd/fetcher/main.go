package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.uber.org/zap"

	"deploy_data_bigquery/internal/config"
	"deploy_data_bigquery/internal/logger"
	"deploy_data_bigquery/internal/observability"
	bigqueryrepo "deploy_data_bigquery/internal/repository/bigquery"
	"deploy_data_bigquery/internal/service"
	facebooksvc "deploy_data_bigquery/internal/service/facebook"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg, err := config.Load("configs/.env")
	if err != nil {
		panic(err)
	}

	if err := logger.Init(cfg.LogEnv, cfg.LogLevel); err != nil {
		panic(err)
	}
	log := logger.GetLogger()

	metrics := observability.NewMetrics()
	healthServer := observability.NewHealthServer(cfg.HealthPort, metrics)
	go func() {
		if err := healthServer.Start(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("health server failed", zap.Error(err))
		}
	}()

	fetcher := facebooksvc.NewFetcher(cfg.FacebookAccessToken)

	rawRepo, err := bigqueryrepo.NewRawRepository(ctx, cfg)
	if err != nil {
		log.Fatal("init raw repo failed", zap.Error(err), zap.String("project_id", cfg.BQProjectID), zap.String("credentials", cfg.GoogleApplicationCredentials))
	}
	defer rawRepo.Close()

	if err := rawRepo.EnsureTables(ctx); err != nil {
		log.Fatal("raw tables check failed", zap.Error(err), zap.String("dataset", cfg.BQDatasetRaw))
	}

	pipeline := service.NewPipeline(cfg, fetcher, rawRepo, rawRepo, rawRepo, metrics)

	if cfg.AppMode == "backfill" {
		log.Info("APP: run mode backfill (one-shot)")
		if err := pipeline.RunBackfill(ctx); err != nil {
			log.Fatal("backfill failed", zap.Error(err))
		}
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		_ = healthServer.Shutdown(shutdownCtx)
		log.Info("APP: backfill completed")
		return
	}

	scheduler := service.NewScheduler(cfg, pipeline)
	if err := scheduler.RegisterJobs(ctx); err != nil {
		log.Fatal("register scheduler failed", zap.Error(err), zap.String("cron", cfg.CronSchedule), zap.String("mode", cfg.AppMode))
	}
	scheduler.Start()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Info("APP: received shutdown signal")
	scheduler.Stop(ctx)

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	_ = healthServer.Shutdown(shutdownCtx)
}
