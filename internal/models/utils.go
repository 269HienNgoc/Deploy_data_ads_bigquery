package models

import (
	"time"

	"cloud.google.com/go/bigquery"
)

// formatTimestamp returns RFC3339 string for valid time, or nil (BQ NULL) for zero time.
// BigQuery TIMESTAMP column cannot parse empty string "".
func formatTimestamp(t time.Time) bigquery.Value {
	if t.IsZero() {
		return nil
	}
	return t.Format(time.RFC3339)
}

// formatDate returns YYYY-MM-DD string for BQ DATE column, or nil for zero time.
func formatDate(t time.Time) bigquery.Value {
	if t.IsZero() {
		return nil
	}
	return t.Format("2006-01-02")
}
