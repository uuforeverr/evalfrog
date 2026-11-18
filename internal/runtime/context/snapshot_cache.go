package context

import (
	"container/list"
	stdcontext "context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/uu999/evalfrog/internal/dsl"
)

type SnapshotCacheMetrics interface {
	ObserveSnapshotL1(outcome string)
	SetSnapshotL1Bytes(int64)
}

type parsedSnapshot struct {
	entryNodeID dsl.NodeID
	nodes       map[string]dsl.Node
	bytes       int64
}

func parseSnapshot(raw json.RawMessage) (*parsedSnapshot, error) {
	var document dsl.Document
	if err := json.Unmarshal(raw, &document); err != nil {
		return nil, fmt.Errorf("decode execution snapshot DSL: %w", err)
	}
	// Charge a conservative parsed-object estimate rather than raw JSON length:
	// maps, strings and decoded values occupy substantially more heap space.
	result := &parsedSnapshot{entryNodeID: document.EntryNodeID, nodes: make(map[string]dsl.Node, len(document.Nodes)), bytes: int64(len(raw))*4 + int64(len(document.Nodes))*256}
	for _, node := range document.Nodes {
		key := string(node.ID)
		if key == "" {
			return nil, fmt.Errorf("execution snapshot contains an empty node id")
		}
		if _, exists := result.nodes[key]; exists {
			return nil, fmt.Errorf("execution snapshot contains duplicate node %q", key)
		}
		result.nodes[key] = node
	}
	return result, nil
}

func (snapshot *parsedSnapshot) node(metadata Metadata) (*dsl.Node, error) {
	node, exists := snapshot.nodes[metadata.ExecutionNodeID]
	if !exists || node.Operation.Coordinate() != metadata.Operation {
		return nil, fmt.Errorf("execution snapshot and persisted node coordinate disagree")
	}
	copy := node
	return &copy, nil
}

type snapshotEntry struct {
	key      string
	snapshot *parsedSnapshot
}

type snapshotLoad struct {
	done     chan struct{}
	snapshot *parsedSnapshot
	err      error
}

type SnapshotCache struct {
	mu         sync.Mutex
	maxEntries int
	maxBytes   int64
	usedBytes  int64
	lru        *list.List
	entries    map[string]*list.Element
	inflight   map[string]*snapshotLoad
	metrics    SnapshotCacheMetrics
}

func NewSnapshotCache(maxEntries int, maxBytes int64, metrics ...SnapshotCacheMetrics) (*SnapshotCache, error) {
	if maxEntries <= 0 || maxBytes <= 0 {
		return nil, fmt.Errorf("snapshot L1 entry and byte limits must be positive")
	}
	if len(metrics) > 1 {
		return nil, fmt.Errorf("at most one snapshot L1 metrics observer is allowed")
	}
	var observer SnapshotCacheMetrics
	if len(metrics) == 1 {
		observer = metrics[0]
	}
	return &SnapshotCache{maxEntries: maxEntries, maxBytes: maxBytes, lru: list.New(), entries: make(map[string]*list.Element), inflight: make(map[string]*snapshotLoad), metrics: observer}, nil
}

func (cache *SnapshotCache) Load(ctx stdcontext.Context, key string, loader func(stdcontext.Context) (json.RawMessage, error)) (*parsedSnapshot, error) {
	cache.mu.Lock()
	if element := cache.entries[key]; element != nil {
		cache.lru.MoveToFront(element)
		snapshot := element.Value.(snapshotEntry).snapshot
		cache.mu.Unlock()
		cache.observe("hit")
		return snapshot, nil
	}
	if pending := cache.inflight[key]; pending != nil {
		cache.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-pending.done:
			return pending.snapshot, pending.err
		}
	}
	pending := &snapshotLoad{done: make(chan struct{})}
	cache.inflight[key] = pending
	cache.mu.Unlock()
	cache.observe("miss")
	raw, err := loader(ctx)
	var snapshot *parsedSnapshot
	if err == nil {
		snapshot, err = parseSnapshot(raw)
	}
	cache.mu.Lock()
	delete(cache.inflight, key)
	if err == nil {
		cache.insertLocked(key, snapshot)
	}
	pending.snapshot, pending.err = snapshot, err
	close(pending.done)
	usedBytes := cache.usedBytes
	cache.mu.Unlock()
	cache.setBytes(usedBytes)
	if err != nil {
		cache.observe("load_error")
	}
	return snapshot, err
}

func (cache *SnapshotCache) insertLocked(key string, snapshot *parsedSnapshot) {
	if snapshot.bytes > cache.maxBytes {
		cache.observe("oversize")
		return
	}
	cache.entries[key] = cache.lru.PushFront(snapshotEntry{key: key, snapshot: snapshot})
	cache.usedBytes += snapshot.bytes
	for cache.lru.Len() > cache.maxEntries || cache.usedBytes > cache.maxBytes {
		oldest := cache.lru.Back()
		entry := oldest.Value.(snapshotEntry)
		delete(cache.entries, entry.key)
		cache.usedBytes -= entry.snapshot.bytes
		cache.lru.Remove(oldest)
		cache.observe("eviction")
	}
}

func (cache *SnapshotCache) observe(outcome string) {
	if cache.metrics != nil {
		cache.metrics.ObserveSnapshotL1(outcome)
	}
}

func (cache *SnapshotCache) setBytes(value int64) {
	if cache.metrics != nil {
		cache.metrics.SetSnapshotL1Bytes(value)
	}
}
