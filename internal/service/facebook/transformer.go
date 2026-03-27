package facebook

import (
	"encoding/json"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"deploy_data_bigquery/internal/models"
)

// ── Campaign category maps ─────────────────────────────────────────────────────

var (
	branchMap       map[string]string
	serviceMap      map[string]string
	typeCampaignMap map[string]string
	categoryOnce    sync.Once
)

func loadCategoryMaps() {
	branchMap = loadMap("category-campagin/branch.json")
	serviceMap = loadMap("category-campagin/service.json")
	typeCampaignMap = loadMap("category-campagin/type-campaign.json")
}

func loadMap(path string) map[string]string {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	var m map[string]string
	if err := json.Unmarshal(data, &m); err == nil {
		return m
	}
	return nil
}

// extractCategories scans campaignName parts separated by " - " and matches
// against branch, service, and type-campaign maps. Returns the value (not key)
// from the JSON maps. Parts are checked in order; first match per category wins.
func extractCategories(campaignName string) (branch, service, typeCampaign string) {
	categoryOnce.Do(loadCategoryMaps)

	parts := strings.Split(campaignName, " - ")
	for _, part := range parts {
		key := strings.TrimSpace(part)
		if branch == "" && branchMap != nil {
			if v, ok := branchMap[key]; ok {
				branch = v
				continue
			}
		}
		if typeCampaign == "" && typeCampaignMap != nil {
			if v, ok := typeCampaignMap[key]; ok {
				typeCampaign = v
				continue
			}
		}
		if service == "" && serviceMap != nil {
			if v, ok := serviceMap[key]; ok {
				service = v
			}
		}
	}
	return
}

// FixedZone for Asia/Ho_Chi_Minh (UTC+7) — does not require tzdata OS library.
var hcmZone = time.FixedZone("Asia/Ho_Chi_Minh", 7*60*60)

// hasTimezoneOffset returns true if s contains an explicit timezone offset
// (e.g. "+0700", "Z"). Used to determine if time.Parse already resolved to UTC.
func hasTimezoneOffset(s string) bool {
	return strings.HasSuffix(s, "Z") ||
		regexp.MustCompile(`[+-]\d{4}$`).MatchString(s)
}

// convertToHCM converts a time value to Asia/Ho_Chi_Minh (UTC+7).
// Time zones have already been resolved by time.Parse during getTime, so this
// only needs to re-interpret the moment in the target timezone.
func convertToHCM(t time.Time) time.Time {
	if t.IsZero() {
		return t
	}
	return t.In(hcmZone)
}

// TransformAccounts transforms raw Facebook API JSON data into []Account models.
func TransformAccounts(data json.RawMessage, flatform string) ([]models.Account, error) {
	var raw []map[string]interface{}
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, err
	}

	accounts := make([]models.Account, 0, len(raw))
	for _, r := range raw {
		a := models.Account{
			ID:            getString(r, "id"),
			Name:          getString(r, "name"),
			AccountStatus: getInt64(r, "account_status"),
			Currency:      getString(r, "currency"),
			TimezoneName:  getString(r, "timezone_name"),
			CreatedTime:   convertToHCM(getTime(r, "created_time")),
			UpdatedTime:   convertToHCM(getTime(r, "updated_time")),
			SpendCap:      getFloat64(r, "spend_cap"),
			AmountSpent:   getFloat64(r, "amount_spent"),
			Flatform:      flatform,
			FetchedAt:     time.Now().UTC(),
		}
		accounts = append(accounts, a)
	}
	return accounts, nil
}

// TransformCampaigns transforms raw Facebook API JSON data into []Campaign models.
func TransformCampaigns(data json.RawMessage, accountID, accountTimezone, flatform string) ([]models.Campaign, error) {
	var raw []map[string]interface{}
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, err
	}

	campaigns := make([]models.Campaign, 0, len(raw))
	for _, r := range raw {
		name := getString(r, "name")
		branch, service, typeCampaign := extractCategories(name)

		c := models.Campaign{
			ID:               getString(r, "id"),
			AccountID:        accountID,
			Name:             name,
			Objective:        getString(r, "objective"),
			Status:           getString(r, "status"),
			ConfiguredStatus: getString(r, "configured_status"),
			EffectiveStatus:  getString(r, "effective_status"),
			BuyingType:       getString(r, "buying_type"),
			DailyBudget:      getFloat64(r, "daily_budget"),    // VND
			LifetimeBudget:   getFloat64(r, "lifetime_budget"), // VND
			StartTime:        convertToHCM(getTime(r, "start_time")),
			StopTime:         convertToHCM(getTime(r, "stop_time")),
			CreatedTime:      convertToHCM(getTime(r, "created_time")),
			UpdatedTime:      convertToHCM(getTime(r, "updated_time")),
			TimezoneName:     accountTimezone,
			Branch:           branch,
			Service:          service,
			TypeCampaign:     typeCampaign,
			Flatform:         flatform,
			FetchedAt:        time.Now().UTC(),
		}
		campaigns = append(campaigns, c)
	}
	return campaigns, nil
}

// TransformInsights transforms raw Facebook insights API JSON data into []Insight models.
func TransformInsights(data json.RawMessage, accountName, flatform string) ([]models.Insight, error) {
	var raw []map[string]interface{}
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, err
	}

	insights := make([]models.Insight, 0, len(raw))
	for _, r := range raw {
		status := getString(r, "campaign_status")
		if status == "" {
			status = "UNKNOWN"
		}

		i := models.Insight{
			Date:         getDate(r, "date_start"),
			Flatform:     flatform,
			AccountID:    normalizeAccountID(getString(r, "account_id")), // insights API trả không có "act_"

			AccountName:  accountName,
			CampaignID:   getString(r, "campaign_id"),
			CampaignName: getString(r, "campaign_name"),
			Status:       status,
			DailyBudget:  getFloat64(r, "daily_budget"), // VND
			Spend:        getFloat64(r, "spend"),
			FetchedAt:    time.Now().UTC(),
		}
		insights = append(insights, i)
	}
	return insights, nil
}

// TransformRawInsights transforms raw Facebook insights API JSON into []RawInsight models.
// RawInsight is stored in ads_raw.raw_flatform_insights.
func TransformRawInsights(data json.RawMessage, accountTimezone string) ([]models.RawInsight, error) {
	var raw []map[string]interface{}
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, err
	}

	insights := make([]models.RawInsight, 0, len(raw))
	for _, r := range raw {
		accountID := getString(r, "account_id")
		if !strings.HasPrefix(accountID, "act_") {
			accountID = "act_" + accountID
		}

		i := models.RawInsight{
			AccountID:       accountID,
			CampaignID:      getString(r, "campaign_id"),
			Spend:          getFloat64(r, "spend"),
			DateStart:       parseDateInTimezone(r, "date_start"),
			DateStop:        parseDateInTimezone(r, "date_stop"),
			AccountTimezone: accountTimezone,
			Flatform:        flatform,
			FetchedAt:       time.Now().UTC(),
		}
		insights = append(insights, i)
	}
	return insights, nil
}

// ── Helpers ──────────────────────────────────────────────────────────────────

func getString(m map[string]interface{}, key string) string {
	if v, ok := m[key].(string); ok {
		return v
	}
	return ""
}

func getInt64(m map[string]interface{}, key string) int64 {
	if v, ok := m[key].(float64); ok {
		return int64(v)
	}
	return 0
}

func getFloat64(m map[string]interface{}, key string) float64 {
	v, ok := m[key]
	if !ok || v == nil {
		return 0
	}
	switch n := v.(type) {
	case float64:
		return n
	case float32:
		return float64(n)
	case int:
		return float64(n)
	case int64:
		return float64(n)
	case json.Number:
		f, err := n.Float64()
		if err != nil {
			return 0
		}
		return f
	case string:
		if n == "" {
			return 0
		}
		f, err := strconv.ParseFloat(n, 64)
		if err != nil {
			return 0
		}
		return f
	default:
		return 0
	}
}

func getTime(m map[string]interface{}, key string) time.Time {
	s := getString(m, key)
	if s == "" {
		return time.Time{}
	}
	// RFC3339 (e.g. "2024-01-01T08:00:00+0000")
	t, err := time.Parse(time.RFC3339, s)
	if err == nil {
		return t
	}
	// Unix timestamp fallback (e.g. "1704067200")
	if unix, err := strconv.ParseInt(s, 10, 64); err == nil {
		return time.Unix(unix, 0).UTC()
	}
	return time.Time{}
}

func getDate(m map[string]interface{}, key string) time.Time {
	s := getString(m, key)
	if s == "" {
		return time.Time{}
	}
	t, err := time.Parse("2006-01-02", s)
	if err != nil {
		return time.Time{}
	}
	return t
}

// parseDateInTimezone parses a YYYY-MM-DD date string and returns time.Time
// interpreted as midnight in Asia/Ho_Chi_Minh timezone.
func parseDateInTimezone(m map[string]interface{}, key string) time.Time {
	s := getString(m, key)
	if s == "" {
		return time.Time{}
	}
	t, err := time.ParseInLocation("2006-01-02", s, hcmZone)
	if err != nil {
		return time.Time{}
	}
	return t
}

// normalizeAccountID prepends "act_" if account ID lacks it.
// Insights API returns bare numeric IDs (e.g. "123456789"),
// while /me/adaccounts returns prefixed IDs (e.g. "act_123456789").
// This ensures JOIN between raw_flatform_insights and raw_flatform_account matches.
func normalizeAccountID(id string) string {
	if id == "" {
		return id
	}
	if !strings.HasPrefix(id, "act_") {
		return "act_" + id
	}
	return id
}
