package metrics

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/uu999/evalfrog/internal/platform/buildinfo"
)

type Registry struct {
	prometheus                  *prometheus.Registry
	Requests                    *prometheus.CounterVec
	OutboxOldestAgeSeconds      prometheus.Gauge
	KafkaConsumerLag            *prometheus.GaugeVec
	LeaseLostTotal              *prometheus.CounterVec
	ReadyToQueuedSeconds        prometheus.Observer
	PostgresPoolAcquireSeconds  *prometheus.HistogramVec
	ExecutionContextCacheAccess *prometheus.CounterVec
	SchedulingRedisRebuildTotal *prometheus.CounterVec
	TopicQueueWindow             *prometheus.GaugeVec
	TopicQueueOccupancy          *prometheus.GaugeVec
	WorkerCompletionEWMA         *prometheus.GaugeVec
	RecoveryWakeupsTotal        *prometheus.CounterVec
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
	readyToQueued := prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "evalfrog", Name: "ready_to_queued_seconds",
		Help:    "PostgreSQL authority latency from a Node becoming Ready to a dispatched Attempt becoming Queued.",
		Buckets: prometheus.DefBuckets,
	})
	postgresPoolAcquire := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "evalfrog", Name: "postgres_pool_acquire_seconds",
		Help:    "Time spent waiting to acquire an authoritative PostgreSQL connection, including failed acquires.",
		Buckets: []float64{0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1},
	}, []string{"outcome"})
	executionContextCache := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "evalfrog", Name: "execution_context_cache_access_total",
		Help: "Cache-aside accesses for bounded execution-context parts; cache failures and invalid data are misses.",
	}, []string{"kind", "outcome"})
	redisRebuild := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "evalfrog", Name: "scheduling_redis_rebuild_total",
		Help: "Scheduling Redis authority-generation rebuild outcomes; a failure leaves growth fail-closed.",
	}, []string{"outcome"})
	topicWindow := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "evalfrog", Name: "scheduler_topic_queue_window",
		Help: "Target queued Task count derived from actual Worker completion throughput.",
	}, []string{"resource_class"})
	topicOccupancy := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "evalfrog", Name: "scheduler_topic_queue_occupancy",
		Help: "Unconfirmed Reservations plus authoritative Queued Attempts.",
	}, []string{"resource_class"})
	completionEWMA := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "evalfrog", Name: "scheduler_worker_completion_ewma_per_second",
		Help: "EWMA of Attempts actually completed by Workers per second.",
	}, []string{"resource_class"})
	recoveryWakeups := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "evalfrog", Name: "runtime_recovery_wakeups_total",
		Help: "Durable recovery wake-up emission outcomes by bounded scanner and Runtime event type.",
	}, []string{"source", "event_type", "outcome"})
	registry.MustRegister(buildGauge, requests, outboxAge, kafkaLag, leaseLost, readyToQueued, postgresPoolAcquire, executionContextCache, redisRebuild, topicWindow, topicOccupancy, completionEWMA, recoveryWakeups)
	return &Registry{prometheus: registry, Requests: requests, OutboxOldestAgeSeconds: outboxAge,
		KafkaConsumerLag: kafkaLag, LeaseLostTotal: leaseLost, ReadyToQueuedSeconds: readyToQueued,
		PostgresPoolAcquireSeconds: postgresPoolAcquire, ExecutionContextCacheAccess: executionContextCache,
		SchedulingRedisRebuildTotal: redisRebuild, TopicQueueWindow: topicWindow,
		TopicQueueOccupancy: topicOccupancy, WorkerCompletionEWMA: completionEWMA,
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

func (registry *Registry) ObserveReadyToQueued(value time.Duration) {
	if value >= 0 {
		registry.ReadyToQueuedSeconds.Observe(value.Seconds())
	}
}

func (registry *Registry) ObservePostgresPoolAcquire(value time.Duration, outcome string) {
	if value >= 0 {
		registry.PostgresPoolAcquireSeconds.WithLabelValues(outcome).Observe(value.Seconds())
	}
}

func (registry *Registry) ObserveExecutionContextCache(kind, outcome string) {
	registry.ExecutionContextCacheAccess.WithLabelValues(kind, outcome).Inc()
}

func (registry *Registry) ObserveSchedulingRedisRebuild(outcome string) {
	registry.SchedulingRedisRebuildTotal.WithLabelValues(outcome).Inc()
}

func (registry *Registry) ObserveTopicQueue(resourceClass string, window, occupancy int, ewma float64) {
	registry.TopicQueueWindow.WithLabelValues(resourceClass).Set(float64(max(window, 0)))
	registry.TopicQueueOccupancy.WithLabelValues(resourceClass).Set(float64(max(occupancy, 0)))
	registry.WorkerCompletionEWMA.WithLabelValues(resourceClass).Set(max(ewma, 0))
}

func (registry *Registry) ObserveRecoveryWakeup(source, eventType, outcome string) {
	registry.RecoveryWakeupsTotal.WithLabelValues(source, eventType, outcome).Inc()
}

func (registry *Registry) Handler() http.Handler {
	return promhttp.HandlerFor(registry.prometheus, promhttp.HandlerOpts{})
}
