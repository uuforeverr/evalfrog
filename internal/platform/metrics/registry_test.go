package metrics

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestRegistryExposesBuildMetric(t *testing.T) {
	t.Parallel()
	registry := New("evalfrog-test")
	registry.ObserveOutboxOldestAge(time.Second)
	registry.ObserveKafkaConsumerLag("runtime-engine-v1", "runtime-events", 2)
	registry.ObserveLeaseLost("reaper")
	registry.ObserveReadyToQueued(time.Millisecond)
	registry.ObservePostgresPoolAcquire(time.Millisecond, "success")
	registry.ObserveExecutionContextCache("snapshot", "hit")
	registry.ObserveSchedulingRedisRebuild("success")
	registry.ObserveTopicQueue("builtin", 10, 3, 2.5)
	registry.ObserveRecoveryWakeup("retry-timer", "retry.due", "emitted")
	request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	response := httptest.NewRecorder()
	registry.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), "evalfrog_build_info") || !strings.Contains(response.Body.String(), "evalfrog_outbox_oldest_unpublished_age_seconds") || !strings.Contains(response.Body.String(), "evalfrog_postgres_pool_acquire_seconds") || !strings.Contains(response.Body.String(), "evalfrog_execution_context_cache_access_total") || !strings.Contains(response.Body.String(), "evalfrog_runtime_recovery_wakeups_total") {
		t.Fatalf("status=%d body=%q", response.Code, response.Body.String())
	}
}
