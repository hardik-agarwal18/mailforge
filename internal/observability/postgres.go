package observability

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// CollectPostgresPoolMetrics samples pgxpool statistics until ctx is canceled.
// Cumulative pgxpool values are intentionally exposed as gauges because pgx
// reports them as process-lifetime snapshots rather than deltas.
func CollectPostgresPoolMetrics(
	ctx context.Context,
	pool *pgxpool.Pool,
	metrics *Metrics,
	interval time.Duration,
) {
	if pool == nil || metrics == nil {
		return
	}
	if interval <= 0 {
		interval = 5 * time.Second
	}

	collect := func() {
		stats := pool.Stat()
		metrics.SetPostgresPoolStats(PostgresPoolSnapshot{
			TotalConnections:        stats.TotalConns(),
			AcquiredConnections:     stats.AcquiredConns(),
			IdleConnections:         stats.IdleConns(),
			ConstructingConnections: stats.ConstructingConns(),
			MaxConnections:          stats.MaxConns(),
			AcquireCount:            stats.AcquireCount(),
			AcquireDurationSeconds:  stats.AcquireDuration().Seconds(),
			EmptyAcquireWaitSeconds: stats.EmptyAcquireWaitTime().Seconds(),
			EmptyAcquireCount:       stats.EmptyAcquireCount(),
			CanceledAcquireCount:    stats.CanceledAcquireCount(),
			NewConnectionsCount:     stats.NewConnsCount(),
		})
	}

	collect()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			collect()
		}
	}
}
