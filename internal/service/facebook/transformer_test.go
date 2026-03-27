package facebook

import (
	"encoding/json"
	"testing"
)

func TestTransformInsights(t *testing.T) {
	raw := []map[string]interface{}{
		{
			"account_id":      "act_1",
			"campaign_id":     "cmp_1",
			"campaign_name":   "Campaign A",
			"campaign_status": "ACTIVE",
			"date_start":      "2026-03-20",
			"daily_budget":    "10000",
			"spend":           "50.5",
		},
	}
	b, _ := json.Marshal(raw)
	got, err := TransformInsights(b, "Account A", "facebook")
	if err != nil {
		t.Fatalf("TransformInsights error: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("expected 1 insight, got %d", len(got))
	}
	if got[0].CampaignID != "cmp_1" {
		t.Fatalf("expected campaign id cmp_1, got %s", got[0].CampaignID)
	}
	if got[0].DailyBudget != 100 {
		t.Fatalf("expected daily_budget 100, got %v", got[0].DailyBudget)
	}
	if got[0].Spend != 50.5 {
		t.Fatalf("expected spend 50.5, got %v", got[0].Spend)
	}
}

func TestGetFloat64_WithString(t *testing.T) {
	m := map[string]interface{}{"v": "123.45"}
	if got := getFloat64(m, "v"); got != 123.45 {
		t.Fatalf("expected 123.45, got %v", got)
	}
}

func TestGetFloat64_WithNumber(t *testing.T) {
	m := map[string]interface{}{"v": 99.9}
	if got := getFloat64(m, "v"); got != 99.9 {
		t.Fatalf("expected 99.9, got %v", got)
	}
}

func TestGetFloat64_WithInvalidString(t *testing.T) {
	m := map[string]interface{}{"v": "abc"}
	if got := getFloat64(m, "v"); got != 0 {
		t.Fatalf("expected 0, got %v", got)
	}
}
