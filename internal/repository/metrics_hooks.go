package repository

import "deploy_data_bigquery/internal/observability"

type MetricsAware interface {
	SetMetrics(metrics *observability.Metrics)
}
