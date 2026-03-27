package models

import (
	"time"

	"cloud.google.com/go/bigquery"
)

// Insight represents aggregated ad performance data for the mart layer.
// Implements bigquery.ValueSaver so field names map explicitly to BQ column names.
type Insight struct {
	Date         time.Time
	Flatform     string
	AccountID    string
	AccountName  string
	CampaignID   string
	CampaignName string
	Status       string
	DailyBudget  float64
	Spend        float64
	FetchedAt    time.Time
}

func (i Insight) Save() (map[string]bigquery.Value, string, error) {
	return map[string]bigquery.Value{
		"date":          i.Date.Format("2006-01-02"), // BQ DATE expects YYYY-MM-DD, not full timestamp
		"flatform":      i.Flatform,
		"account_id":    i.AccountID,
		"account_name":  i.AccountName,
		"campaign_id":   i.CampaignID,
		"campaign_name": i.CampaignName,
		"status":        i.Status,
		"daily_budget":  i.DailyBudget,
		"spend":         i.Spend,
		"fetched_at":    i.FetchedAt,
	}, "", nil
}

// TimeRange represents a date range for API queries.
type TimeRange struct {
	Since string `json:"since"`
	Until string `json:"until"`
}
