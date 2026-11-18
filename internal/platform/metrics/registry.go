package metrics

import (
	"database/sql"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/uu999/evalfrog/internal/platform/buildinfo"
)

type Registry struct {
	prometheus              *prometheus.Registry
	Requests                *prometheus.CounterVec
	OutboxOldestAgeSeconds  prometheus.Gauge
	KafkaConsumerLag        *prometheus.GaugeVec
	LeaseLostTotal          *prometheus.CounterVec
	MySQLPoolAcquireSeconds *prometheus.HistogramVec
	SnapshotL1Access        *prometheus.CounterVec
	SnapshotL1Bytes         prometheus.Gauge
	RecoveryWakeupsTotal    *prometheus.CounterVec
}

func New(service string) *Registry {
	registry := prometheus.NewRegistry()
	registry.MustRegister(prometheus.NewGoCollector(), prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))
	build := buildinfo.Current()
	buildGauge := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "evalfrog",
		Name:      "build_info",
		Help:      "Static build metadata for the running EvalFrog process.",
	}, []string{"service", "version", "commit"})
	buildGauge.WithLabelValues(service, build.Version, build.Commit).Set(1)
	requests := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "evalfrog",
		Name:      "http_requests_total",
		Help:      "HTTP requests handled by the M0 process shell.",
	}, []string{"service", "route", "status"})
	outboxAge := prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "evalfrog", Name: "outbox_oldest_unpublished_age_seconds",
		Help: "Age of the oldest unpublished authoritative Runtime or Task Outbox event.",
	})
	kafkaLag := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "evalfrog", Name: "kafka_consumer_lag_records",
		Help: "Broker end offset minus committed consumer offset, sampled per bounded group and topic.",
	}, []string{"group", "topic"})
	leaseLost := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "evalfrog", Name: "attempt_lease_lost_total",
		Help: "Attempts authoritatively marked lost after lease expiry.",
	}, []string{"source"})
	mysqlPoolAcquire := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "evalfrog", Name: "mysql_pool_acquire_seconds",
		Help:    "Time spent beginning an authoritative MySQL transaction or explicit health-check acquire, including failures.",
		Buckets: []float64{0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1},
	}, []string{"outcome"})
	snapshotL1Access := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "evalfrog", Name: "snapshot_l1_access_total",
		Help: "Process-local immutable parsed Snapshot cache outcomes.",
	}, []string{"outcome"})
	snapshotL1Bytes := prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "evalfrog", Name: "snapshot_l1_bytes",
		Help: "Estimated parsed heap bytes represented by entries currently held in the bounded Snapshot L1.",
	})
	recoveryWakeups := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "evalfrog", Name: "runtime_recovery_wakeups_total",
		Help: "Durable recovery wake-up emission outcomes by bounded scanner and Runtime event type.",
	}, []string{"source", "event_type", "outcome"})
	registry.MustRegister(buildGauge, requests, outboxAge, kafkaLag, leaseLost, mysqlPoolAcquire, snapshotL1Access, snapshotL1Bytes, recoveryWakeups)
	return &Registry{prometheus: registry, Requests: requests, OutboxOldestAgeSeconds: outboxAge,
		KafkaConsumerLag: kafkaLag, LeaseLostTotal: leaseLost,
		MySQLPoolAcquireSeconds: mysqlPoolAcquire, SnapshotL1Access: snapshotL1Access, SnapshotL1Bytes: snapshotL1Bytes,
		RecoveryWakeupsTotal: recoveryWakeups}
}

func (registry *Registry) ObserveOutboxOldestAge(value time.Duration) {
	registry.OutboxOldestAgeSeconds.Set(value.Seconds())
}

func (registry *Registry) ObserveKafkaConsumerLag(group, topic string, value int64) {
	registry.KafkaConsumerLag.WithLabelValues(group, topic).Set(float64(max(value, 0)))
}

func (registry *Registry) ObserveLeaseLost(source string) {
	registry.LeaseLostTotal.WithLabelValues(source).Inc()
}

func (registry *Registry) ObserveMySQLPoolAcquire(value time.Duration, outcome string) {
	if value >= 0 {
		registry.MySQLPoolAcquireSeconds.WithLabelValues(outcome).Observe(value.Seconds())
	}
}

// RegisterMySQLPoolStats samples database/sql counters on scrape, avoiding a
// background polling loop and exposing actual pool wait separately from the
// transaction-begin latency histogram.
func (registry *Registry) RegisterMySQLPoolStats(stats func() sql.DBStats) {
	registry.prometheus.MustRegister(
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{Namespace: "evalfrog", Name: "mysql_pool_open_connections", Help: "Open MySQL connections in this process."}, func() float64 { return float64(stats().OpenConnections) }),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{Namespace: "evalfrog", Name: "mysql_pool_in_use_connections", Help: "MySQL connections currently in use by this process."}, func() float64 { return float64(stats().InUse) }),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{Namespace: "evalfrog", Name: "mysql_pool_idle_connections", Help: "Idle MySQL connections in this process."}, func() float64 { return float64(stats().Idle) }),
		prometheus.NewCounterFunc(prometheus.CounterOpts{Namespace: "evalfrog", Name: "mysql_pool_wait_count_total", Help: "Total database/sql waits for a MySQL connection."}, func() float64 { return float64(stats().WaitCount) }),
		prometheus.NewCounterFunc(prometheus.CounterOpts{Namespace: "evalfrog", Name: "mysql_pool_wait_seconds_total", Help: "Total time spent waiting for a MySQL connection."}, func() float64 { return stats().WaitDuration.Seconds() }),
	)
}

func (registry *Registry) ObserveSnapshotL1(outcome string) {
	registry.SnapshotL1Access.WithLabelValues(outcome).Inc()
}

func (registry *Registry) SetSnapshotL1Bytes(value int64) {
	registry.SnapshotL1Bytes.Set(float64(max(value, 0)))
}

func (registry *Registry) ObserveRecoveryWakeup(source, eventType, outcome string) {
	registry.RecoveryWakeupsTotal.WithLabelValues(source, eventType, outcome).Inc()
}

func (registry *Registry) Handler() http.Handler {
	return promhttp.HandlerFor(registry.prometheus, promhttp.HandlerOpts{})
}
