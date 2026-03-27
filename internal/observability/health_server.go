package observability

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

type HealthServer struct {
	server  *http.Server
	metrics *Metrics
}

func NewHealthServer(port string, metrics *Metrics) *HealthServer {
	mux := http.NewServeMux()
	hs := &HealthServer{metrics: metrics}

	mux.HandleFunc("/health", hs.handleHealth)
	mux.HandleFunc("/metrics", hs.handleMetrics)

	hs.server = &http.Server{
		Addr:              ":" + port,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	return hs
}

func (h *HealthServer) Start() error {
	if err := h.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

func (h *HealthServer) Shutdown(ctx context.Context) error {
	return h.server.Shutdown(ctx)
}

func (h *HealthServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	s := h.metrics.Snapshot()
	status := http.StatusOK
	if s.LastRunStatus == "failed" {
		status = http.StatusServiceUnavailable
	}

	body := map[string]interface{}{
		"status":            s.LastRunStatus,
		"last_run_at":       s.LastRunAt,
		"last_run_duration": s.LastRunDuration.String(),
		"runs_total":        s.RunsTotal,
		"runs_failed":       s.RunsFailed,
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func (h *HealthServer) handleMetrics(w http.ResponseWriter, r *http.Request) {
	s := h.metrics.Snapshot()
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	_, _ = fmt.Fprintf(w, "runs_total %d\n", s.RunsTotal)
	_, _ = fmt.Fprintf(w, "runs_failed %d\n", s.RunsFailed)
	_, _ = fmt.Fprintf(w, "accounts_fetched %d\n", s.AccountsFetched)
	_, _ = fmt.Fprintf(w, "campaigns_fetched %d\n", s.CampaignsFetched)
	_, _ = fmt.Fprintf(w, "insights_fetched %d\n", s.InsightsFetched)
	_, _ = fmt.Fprintf(w, "rows_inserted_raw %d\n", s.RowsInsertedRaw)
	_, _ = fmt.Fprintf(w, "rows_inserted_mart %d\n", s.RowsInsertedMart)
	_, _ = fmt.Fprintf(w, "dry_run_skips %d\n", s.DryRunSkips)
}
