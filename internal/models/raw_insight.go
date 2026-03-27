package models

import (
	"time"

	"cloud.google.com/go/bigquery"
)

// RawInsight represents raw Facebook campaign insights data.
// Stored in ads_raw.raw_flatform_insights — raw layer only, not mart.
// Implements bigquery.ValueSaver so field names map explicitly to BQ column names.
type RawInsight struct {
	AccountID       string
	CampaignID      string
	Spend           float64
	DateStart       time.Time
	DateStop        time.Time
	AccountTimezone string
	Flatform        string
	FetchedAt       time.Time
}

func (r RawInsight) Save() (map[string]bigquery.Value, string, error) {
	return map[string]bigquery.Value{
		"account_id":        r.AccountID,
		"campaign_id":       r.CampaignID,
		"spend":            r.Spend,
		"date_start":       formatDate(r.DateStart),
		"date_stop":        formatDate(r.DateStop),
		"account_timezone": r.AccountTimezone,
		"flatform":         r.Flatform,
		"fetched_at":       formatTimestamp(r.FetchedAt),
	}, "", nil
}
