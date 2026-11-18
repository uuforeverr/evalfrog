package metrics

import (
	"database/sql"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestRegistryExposesBuildMetric(t *testing.T) {
	t.Parallel()
	registry := New("evalfrog-test")
	registry.RegisterMySQLPoolStats(func() sql.DBStats {
		return sql.DBStats{OpenConnections: 3, InUse: 2, Idle: 1, WaitCount: 4, WaitDuration: time.Millisecond}
	})
	registry.ObserveOutboxOldestAge(time.Second)
	registry.ObserveKafkaConsumerLag("runtime-engine-v1", "runtime-events", 2)
	registry.ObserveLeaseLost("reaper")
	registry.ObserveMySQLPoolAcquire(time.Millisecond, "success")
	registry.ObserveSnapshotL1("hit")
	registry.SetSnapshotL1Bytes(1024)
	registry.ObserveRecoveryWakeup("retry-timer", "retry.due", "emitted")
	request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	response := httptest.NewRecorder()
	registry.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), "evalfrog_build_info") || !strings.Contains(response.Body.String(), "evalfrog_outbox_oldest_unpublished_age_seconds") || !strings.Contains(response.Body.String(), "evalfrog_mysql_pool_acquire_seconds") || !strings.Contains(response.Body.String(), "evalfrog_mysql_pool_wait_seconds_total") || !strings.Contains(response.Body.String(), "evalfrog_mysql_pool_in_use_connections") || !strings.Contains(response.Body.String(), "evalfrog_snapshot_l1_access_total") || !strings.Contains(response.Body.String(), "evalfrog_snapshot_l1_bytes") || !strings.Contains(response.Body.String(), "evalfrog_runtime_recovery_wakeups_total") {
		t.Fatalf("status=%d body=%q", response.Code, response.Body.String())
	}
}
