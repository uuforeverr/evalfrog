package context

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"testing"
)

func TestSnapshotCacheCoalescesConcurrentLoads(t *testing.T) {
	cache, err := NewSnapshotCache(8, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	var loads atomic.Int32
	started := make(chan struct{})
	release := make(chan struct{})
	loader := func(context.Context) (json.RawMessage, error) {
		if loads.Add(1) == 1 {
			close(started)
		}
		<-release
		return snapshotFixture(), nil
	}
	const callers = 16
	errors := make(chan error, callers)
	var wait sync.WaitGroup
	for index := 0; index < callers; index++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			_, loadErr := cache.Load(context.Background(), "snapshot", loader)
			errors <- loadErr
		}()
	}
	<-started
	close(release)
	wait.Wait()
	close(errors)
	for loadErr := range errors {
		if loadErr != nil {
			t.Fatal(loadErr)
		}
	}
	if loads.Load() != 1 {
		t.Fatalf("loader calls=%d", loads.Load())
	}
}

func TestSnapshotCacheEvictsByEntryLimit(t *testing.T) {
	metrics := &snapshotMetricsStub{outcomes: make(map[string]int)}
	cache, err := NewSnapshotCache(1, 1<<20, metrics)
	if err != nil {
		t.Fatal(err)
	}
	load := func(context.Context) (json.RawMessage, error) { return snapshotFixture(), nil }
	if _, err = cache.Load(context.Background(), "one", load); err != nil {
		t.Fatal(err)
	}
	if _, err = cache.Load(context.Background(), "two", load); err != nil {
		t.Fatal(err)
	}
	if _, err = cache.Load(context.Background(), "one", load); err != nil {
		t.Fatal(err)
	}
	if metrics.outcomes["eviction"] != 2 || metrics.outcomes["miss"] != 3 {
		t.Fatalf("outcomes=%v", metrics.outcomes)
	}
}

func TestSnapshotCacheDoesNotRetainOversizeSnapshot(t *testing.T) {
	raw := snapshotFixture()
	metrics := &snapshotMetricsStub{outcomes: make(map[string]int)}
	cache, err := NewSnapshotCache(8, int64(len(raw)-1), metrics)
	if err != nil {
		t.Fatal(err)
	}
	loads := 0
	loader := func(context.Context) (json.RawMessage, error) {
		loads++
		return raw, nil
	}
	for index := 0; index < 2; index++ {
		if _, err = cache.Load(context.Background(), "large", loader); err != nil {
			t.Fatal(err)
		}
	}
	if loads != 2 || metrics.outcomes["oversize"] != 2 || metrics.bytes != 0 {
		t.Fatalf("loads=%d outcomes=%v bytes=%d", loads, metrics.outcomes, metrics.bytes)
	}
}

type snapshotMetricsStub struct {
	outcomes map[string]int
	bytes    int64
}

func (metrics *snapshotMetricsStub) ObserveSnapshotL1(outcome string) {
	metrics.outcomes[outcome]++
}

func (metrics *snapshotMetricsStub) SetSnapshotL1Bytes(value int64) {
	metrics.bytes = value
}
