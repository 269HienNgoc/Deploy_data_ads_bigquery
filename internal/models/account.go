package models

import (
	"time"

	"cloud.google.com/go/bigquery"
)

// Account represents a Facebook Ad Account.
// Implements bigquery.ValueSaver so field names map explicitly to BQ column names.
type Account struct {
	ID            string
	Name          string
	AccountStatus int64
	Currency      string
	TimezoneName  string
	CreatedTime   time.Time
	UpdatedTime   time.Time
	SpendCap      float64
	AmountSpent   float64
	Flatform      string
	FetchedAt     time.Time
}

func (a Account) Save() (map[string]bigquery.Value, string, error) {
	return map[string]bigquery.Value{
		"id":             a.ID,
		"name":           a.Name,
		"account_status": a.AccountStatus,
		"currency":       a.Currency,
		"timezone_name":  a.TimezoneName,
		"created_time":   a.CreatedTime,
		"updated_time":   a.UpdatedTime,
		"spend_cap":      a.SpendCap,
		"amount_spent":   a.AmountSpent,
		"flatform":       a.Flatform,
		"fetched_at":     a.FetchedAt,
	}, "", nil
}
