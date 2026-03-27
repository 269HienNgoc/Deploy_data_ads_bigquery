package repository

import (
	"context"

	"deploy_data_bigquery/internal/models"
)

// AccountRepository handles account persistence.
type AccountRepository interface {
	InsertAccounts(ctx context.Context, accounts []models.Account) error
}

// CampaignRepository handles campaign persistence.
type CampaignRepository interface {
	InsertCampaigns(ctx context.Context, campaigns []models.Campaign) error
}

// RawInsightRepository handles raw Facebook insights persistence (ads_raw.raw_flatform_insights).
type RawInsightRepository interface {
	InsertInsights(ctx context.Context, insights []models.RawInsight) error
	RefreshLatestTables(ctx context.Context) error
}
