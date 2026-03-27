package models

import (
	"time"

	"cloud.google.com/go/bigquery"
)

// Campaign represents a Facebook Ad Campaign.
// Implements bigquery.ValueSaver so field names map explicitly to BQ column names.
type Campaign struct {
	ID               string
	AccountID        string
	Name             string
	Objective        string
	Status           string
	ConfiguredStatus string
	EffectiveStatus  string
	BuyingType       string
	DailyBudget      float64
	LifetimeBudget   float64
	StartTime        time.Time
	StopTime         time.Time
	CreatedTime      time.Time
	UpdatedTime      time.Time
	TimezoneName     string
	Branch           string // e.g. "Bệnh viện", "Đăng lưu", "Hệ thống"
	Service          string // e.g. "TIN NHẮN", "TRUY CẬP", "TIẾP CẬN", "SEARCH", "PMAX", "VIEW VIDEO"
	TypeCampaign     string // e.g. "TRỒNG RĂNG", "BỌC SỨ", "NIỀNG RĂNG", "KM THÁNG", "NHỔ RĂNG KHÔN"
	Flatform         string
	FetchedAt        time.Time
}

func (c Campaign) Save() (map[string]bigquery.Value, string, error) {
	return map[string]bigquery.Value{
		"id":                c.ID,
		"account_id":        c.AccountID,
		"name":              c.Name,
		"objective":         c.Objective,
		"status":            c.Status,
		"configured_status": c.ConfiguredStatus,
		"effective_status":  c.EffectiveStatus,
		"buying_type":       c.BuyingType,
		"daily_budget":      c.DailyBudget,
		"lifetime_budget":   c.LifetimeBudget,
		"start_time":        c.StartTime,
		"stop_time":         c.StopTime,
		"created_time":      c.CreatedTime,
		"updated_time":      c.UpdatedTime,
		"timezone_name":     c.TimezoneName,
		"branch":            c.Branch,
		"service":           c.Service,
		"type_campaign":     c.TypeCampaign,
		"flatform":          c.Flatform,
		"fetched_at":        c.FetchedAt,
	}, "", nil
}
