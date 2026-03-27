package config

import (
	"fmt"
	"strings"
	"time"

	"github.com/spf13/viper"
)

// Config holds all application configuration.
type Config struct {
	FacebookAccessToken          string
	GoogleApplicationCredentials string
	BQProjectID                  string
	BQDatasetRaw                 string
	BQDatasetMart                string
	AppMode                      string // "daily" | "backfill"
	BackfillSince                string
	MaxWorkers                   int
	BatchSize                    int
	CronSchedule                 string
	MaxRetries                   int
	RetryBaseDelay               time.Duration
	DryRun                       bool
	HealthPort                   string
	LogLevel                     string
	LogEnv                       string
}

// Load reads config from the specified .env file path.
func Load(configPath string) (*Config, error) {
	v := viper.New()
	v.SetConfigFile(configPath)
	v.SetConfigType("env")
	v.AutomaticEnv()
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	if err := v.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}

	retryDelay, err := time.ParseDuration(v.GetString("RETRY_BASE_DELAY"))
	if err != nil {
		retryDelay = time.Second
	}

	cfg := &Config{
		FacebookAccessToken:          v.GetString("FACEBOOK_ACCESS_TOKEN"),
		GoogleApplicationCredentials: v.GetString("GOOGLE_APPLICATION_CREDENTIALS"),
		BQProjectID:                  v.GetString("BQ_PROJECT_ID"),
		BQDatasetRaw:                 v.GetString("BQ_DATASET_RAW"),
		BQDatasetMart:                v.GetString("BQ_DATASET_MART"),
		AppMode:                      v.GetString("APP_MODE"),
		BackfillSince:                v.GetString("RUN_BACKFILL_SINCE"),
		MaxWorkers:                   v.GetInt("MAX_WORKERS"),
		BatchSize:                    v.GetInt("BATCH_SIZE"),
		CronSchedule:                 v.GetString("CRON_SCHEDULE"),
		MaxRetries:                   v.GetInt("MAX_RETRIES"),
		RetryBaseDelay:               retryDelay,
		DryRun:                       v.GetBool("APP_DRY_RUN"),
		HealthPort:                   v.GetString("HEALTH_PORT"),
		LogLevel:                     v.GetString("LOG_LEVEL"),
		LogEnv:                       v.GetString("LOG_ENV"),
	}

	// Defaults
	if cfg.MaxWorkers == 0 {
		cfg.MaxWorkers = 10
	}
	if cfg.BatchSize == 0 {
		cfg.BatchSize = 500
	}
	if cfg.MaxRetries == 0 {
		cfg.MaxRetries = 3
	}
	if cfg.RetryBaseDelay == 0 {
		cfg.RetryBaseDelay = time.Second
	}
	if cfg.LogLevel == "" {
		cfg.LogLevel = "info"
	}
	if cfg.LogEnv == "" {
		cfg.LogEnv = "development"
	}
	if cfg.CronSchedule == "" {
		cfg.CronSchedule = "0 1,8,14,17 * * *"
	}
	if cfg.BackfillSince == "" {
		cfg.BackfillSince = "2025-01-01"
	}
	if cfg.HealthPort == "" {
		cfg.HealthPort = "8080"
	}

	return cfg, nil
}
