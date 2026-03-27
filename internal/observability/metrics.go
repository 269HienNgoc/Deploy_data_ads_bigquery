package observability

import (
	"sync"
	"time"
)

type Metrics struct {
	mu sync.RWMutex

	RunsTotal        int64
	RunsFailed       int64
	LastRunAt        time.Time
	LastRunDuration  time.Duration
	LastRunStatus    string
	AccountsFetched  int64
	CampaignsFetched int64
	InsightsFetched  int64
	RowsInsertedRaw  int64
	RowsInsertedMart int64
	DryRunSkips      int64
}

func NewMetrics() *Metrics {
	return &Metrics{LastRunStatus: "idle"}
}

func (m *Metrics) MarkRunStart() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.RunsTotal++
	m.LastRunAt = time.Now().UTC()
	m.LastRunStatus = "running"
}

func (m *Metrics) MarkRunSuccess(duration time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.LastRunDuration = duration
	m.LastRunStatus = "success"
}

func (m *Metrics) MarkRunFailed(duration time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.RunsFailed++
	m.LastRunDuration = duration
	m.LastRunStatus = "failed"
}

func (m *Metrics) AddAccountsFetched(n int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.AccountsFetched += int64(n)
}

func (m *Metrics) AddCampaignsFetched(n int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.CampaignsFetched += int64(n)
}

func (m *Metrics) AddInsightsFetched(n int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.InsightsFetched += int64(n)
}

func (m *Metrics) AddRowsInsertedRaw(n int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.RowsInsertedRaw += int64(n)
}

func (m *Metrics) AddRowsInsertedMart(n int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.RowsInsertedMart += int64(n)
}

func (m *Metrics) AddDryRunSkip(n int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.DryRunSkips += int64(n)
}

func (m *Metrics) Snapshot() Metrics {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return Metrics{
		RunsTotal:        m.RunsTotal,
		RunsFailed:       m.RunsFailed,
		LastRunAt:        m.LastRunAt,
		LastRunDuration:  m.LastRunDuration,
		LastRunStatus:    m.LastRunStatus,
		AccountsFetched:  m.AccountsFetched,
		CampaignsFetched: m.CampaignsFetched,
		InsightsFetched:  m.InsightsFetched,
		RowsInsertedRaw:  m.RowsInsertedRaw,
		RowsInsertedMart: m.RowsInsertedMart,
		DryRunSkips:      m.DryRunSkips,
	}
}
