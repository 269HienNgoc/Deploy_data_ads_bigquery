package facebook

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"time"

	"go.uber.org/zap"

	"deploy_data_bigquery/internal/logger"
	"deploy_data_bigquery/internal/models"
)

const flatform = "facebook"

// Fetcher fetches data from the Facebook Marketing API.
type Fetcher struct {
	client *Client
}

// NewFetcher creates a new Facebook Fetcher.
func NewFetcher(accessToken string) *Fetcher {
	return &Fetcher{client: NewClient(accessToken)}
}

// FetchAccounts retrieves all ad accounts for the authenticated user.
func (f *Fetcher) FetchAccounts(ctx context.Context) ([]models.Account, error) {
	log := logger.GetLogger()
	log.Info("FETCHER: fetching ad accounts")

	params := url.Values{}
	params.Set("fields", "id,name,account_status,currency,timezone_name,created_time,updated_time,spend_cap,amount_spent")

	var accounts []models.Account
	err := f.client.paginate(ctx, "/me/adaccounts", params, func(data json.RawMessage) error {
		transformed, err := TransformAccounts(data, flatform)
		if err != nil {
			return fmt.Errorf("transform accounts: %w", err)
		}
		accounts = append(accounts, transformed...)
		log.Debug("FETCHER: accounts batch",
			zap.Int("batch_size", len(transformed)),
			zap.Int("total_so_far", len(accounts)),
		)
		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("fetch accounts: %w", err)
	}

	log.Info("FETCHER: accounts fetched",
		zap.Int("total_accounts", len(accounts)),
	)
	return accounts, nil
}

// FetchCampaigns retrieves all campaigns for a given ad account.
func (f *Fetcher) FetchCampaigns(ctx context.Context, accountID, accountTimezone string) ([]models.Campaign, error) {
	log := logger.GetLogger()
	log.Debug("FETCHER: fetching campaigns",
		zap.String("account_id", accountID),
	)

	params := url.Values{}
	params.Set("fields", "id,name,objective,status,configured_status,effective_status,buying_type,daily_budget,lifetime_budget,start_time,stop_time,created_time,updated_time")

	var campaigns []models.Campaign
	err := f.client.paginate(ctx, "/"+accountID+"/campaigns", params, func(data json.RawMessage) error {
		transformed, err := TransformCampaigns(data, accountID, accountTimezone, flatform)
		if err != nil {
			return fmt.Errorf("transform campaigns: %w", err)
		}
		campaigns = append(campaigns, transformed...)
		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("fetch campaigns for %s: %w", accountID, err)
	}

	log.Debug("FETCHER: campaigns fetched",
		zap.String("account_id", accountID),
		zap.Int("total_campaigns", len(campaigns)),
	)
	return campaigns, nil
}

// FetchInsights retrieves insight data for a given account within a time range.
// datePreset is optional; if not empty, it overrides timeRange.
func (f *Fetcher) FetchInsights(ctx context.Context, accountID, accountName string, datePreset string, timeRange models.TimeRange) ([]models.Insight, error) {
	log := logger.GetLogger()
	log.Debug("FETCHER: fetching insights",
		zap.String("account_id", accountID),
		zap.String("date_preset", datePreset),
		zap.String("time_range_since", timeRange.Since),
		zap.String("time_range_until", timeRange.Until),
	)

	params := url.Values{}
	params.Set("level", "campaign")
	// NOTE: insights endpoint does not support campaign_status/daily_budget in fields.
	// Keep only valid insight fields; status/budget can be enriched from campaign endpoint if needed.
	params.Set("fields", "account_id,account_name,campaign_id,campaign_name,date_start,date_stop,spend")

	if datePreset != "" {
		params.Set("date_preset", datePreset)
	} else if timeRange.Since != "" && timeRange.Until != "" {
		// Encode time_range as JSON (API requirement)
		tr, _ := json.Marshal(timeRange)
		params.Set("time_range", string(tr))
	}

	var insights []models.Insight
	err := f.client.paginate(ctx, "/"+accountID+"/insights", params, func(data json.RawMessage) error {
		transformed, err := TransformInsights(data, accountName, flatform)
		if err != nil {
			return fmt.Errorf("transform insights: %w", err)
		}
		insights = append(insights, transformed...)
		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("fetch insights for %s: %w", accountID, err)
	}

	log.Debug("FETCHER: insights fetched",
		zap.String("account_id", accountID),
		zap.Int("total_insights", len(insights)),
	)
	return insights, nil
}

// FetchRawInsights fetches raw campaign insights from Facebook Marketing API.
// Returns []models.RawInsight for ads_raw.raw_flatform_insights.
// datePreset is optional (e.g. "today", "yesterday"); if empty, timeRange is used.
func (f *Fetcher) FetchRawInsights(ctx context.Context, accountID, accountTimezone, datePreset string, timeRange models.TimeRange) ([]models.RawInsight, error) {
	log := logger.GetLogger()
	log.Debug("FETCHER: fetching raw insights",
		zap.String("account_id", accountID),
		zap.String("date_preset", datePreset),
	)

	params := url.Values{}
	params.Set("level", "campaign")
	params.Set("fields", "account_id,campaign_id,spend,date_start,date_stop")

	if datePreset != "" {
		params.Set("date_preset", datePreset)
	} else if timeRange.Since != "" && timeRange.Until != "" {
		tr, _ := json.Marshal(timeRange)
		params.Set("time_range", string(tr))
	}

	var insights []models.RawInsight
	err := f.client.paginate(ctx, "/"+accountID+"/insights", params, func(data json.RawMessage) error {
		transformed, err := TransformRawInsights(data, accountTimezone)
		if err != nil {
			return fmt.Errorf("transform raw insights: %w", err)
		}
		insights = append(insights, transformed...)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("fetch raw insights for %s: %w", accountID, err)
	}

	log.Debug("FETCHER: raw insights fetched",
		zap.String("account_id", accountID),
		zap.Int("total", len(insights)),
	)
	return insights, nil
}

// FetchAllInsightsForDate fetches insights for a specific date preset (e.g. "yesterday", "today").
func (f *Fetcher) FetchAllInsightsForDate(ctx context.Context, accounts []models.Account, datePreset string) ([]models.Insight, error) {
	var allInsights []models.Insight

	for _, acc := range accounts {
		insights, err := f.FetchInsights(ctx, acc.ID, acc.Name, datePreset, models.TimeRange{})
		if err != nil {
			logger.GetLogger().Error("FETCHER: failed to fetch insights",
				zap.String("account_id", acc.ID),
				zap.Error(err),
			)
			continue // Don't fail the whole job; log and continue
		}
		allInsights = append(allInsights, insights...)
	}

	return allInsights, nil
}

// GenerateBackfillMonths generates a list of (year, month) pairs from startDate to now.
// Facebook allows up to 37 months of backfill.
func GenerateBackfillMonths(startDate string) [][2]int {
	var months [][2]int
	start, err := time.Parse("2006-01-02", startDate)
	if err != nil {
		return months
	}

	now := time.Now()
	for y, m := start.Year(), int(start.Month()); ; {
		months = append(months, [2]int{y, m})
		m++
		if m > 12 {
			m = 1
			y++
		}
		if y > now.Year() || (y == now.Year() && m > int(now.Month())) {
			break
		}
		// Safety cap: max 37 months
		if len(months) >= 37 {
			break
		}
	}
	return months
}

// MonthTimeRange returns the first and last day of a given year/month as strings.
func MonthTimeRange(year, month int) models.TimeRange {
	start := time.Date(year, time.Month(month), 1, 0, 0, 0, 0, time.UTC)
	end := start.AddDate(0, 1, -1)
	return models.TimeRange{
		Since: start.Format("2006-01-02"),
		Until: end.Format("2006-01-02"),
	}
}

// GenerateBackfillDays generates a list of date strings from startDate to today.
func GenerateBackfillDays(startDate string) ([]string, error) {
	start, err := time.Parse("2006-01-02", startDate)
	if err != nil {
		return nil, fmt.Errorf("parse backfill since date: %w", err)
	}
	now := time.Now().UTC()
	var days []string
	for d := start; !d.After(now); d = d.AddDate(0, 0, 1) {
		days = append(days, d.Format("2006-01-02"))
	}
	// Safety cap: max 365 days
	if len(days) > 365 {
		days = days[:365]
	}
	return days, nil
}

// DayTimeRange returns a TimeRange for a single day.
func DayTimeRange(date string) models.TimeRange {
	return models.TimeRange{Since: date, Until: date}
}
