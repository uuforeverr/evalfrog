package scheduling

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/uu999/evalfrog/internal/dsl"
)

var (
	ErrAdmissionPaused = errors.New("scheduling admission is paused")
	ErrCandidateStale  = errors.New("ready candidate is stale")
	ErrLeaseLost       = errors.New("scheduler reconciliation lease is lost")
	ErrMemoryPressure  = errors.New("scheduling Redis is draining under memory pressure")
)

type ResourceClass string

const (
	ResourceBuiltin ResourceClass = "builtin"
	ResourceSandbox ResourceClass = "sandbox"
	ReadyBucketSize               = time.Second
)

func ResourceClasses() []ResourceClass { return []ResourceClass{ResourceBuiltin, ResourceSandbox} }

func (value ResourceClass) Valid() bool {
	return value == ResourceBuiltin || value == ResourceSandbox
}

type Router interface {
	Resolve(dsl.Coordinate) (ResourceClass, bool)
}

type StaticRouter map[dsl.Coordinate]ResourceClass

func (router StaticRouter) Resolve(coordinate dsl.Coordinate) (ResourceClass, bool) {
	value, exists := router[coordinate]
	return value, exists
}

func BuiltinV1Router() StaticRouter {
	return StaticRouter{
		{Type: "task.python", Version: 1}: ResourceSandbox,
		{Type: "task.http", Version: 1}:   ResourceBuiltin,
		{Type: "task.rpc", Version: 1}:    ResourceBuiltin,
	}
}

func RequiredCapabilities(class ResourceClass) []dsl.Coordinate {
	result := make([]dsl.Coordinate, 0, 2)
	for coordinate, routedClass := range BuiltinV1Router() {
		if routedClass == class {
			result = append(result, coordinate)
		}
	}
	sort.Slice(result, func(left, right int) bool {
		if result[left].Type != result[right].Type {
			return result[left].Type < result[right].Type
		}
		return result[left].Version < result[right].Version
	})
	return result
}

func CapabilityFingerprint(class ResourceClass) string {
	digest := sha256.New()
	for _, coordinate := range RequiredCapabilities(class) {
		_, _ = fmt.Fprintf(digest, "%s@%d\n", coordinate.Type, coordinate.Version)
	}
	return hex.EncodeToString(digest.Sum(nil))
}

// Candidate is a PostgreSQL-authoritative Ready node mirrored into Scheduling
// Redis. ReadyBucket is derived from ReadyAt and persisted in Redis only so the
// Lua hot path never has to parse timestamps.
type Candidate struct {
	ProjectID       string        `json:"project_id"`
	RunID           string        `json:"run_id"`
	NodeRunID       string        `json:"node_run_id"`
	ExecutionNodeID string        `json:"execution_node_id"`
	StateVersion    uint64        `json:"state_version"`
	Priority        int           `json:"priority"`
	ReadyAt         time.Time     `json:"ready_at"`
	ReadyBucket     int64         `json:"ready_bucket"`
	ReadyOrderKey   string        `json:"ready_order_key"`
	ResourceClass   ResourceClass `json:"resource_class"`
}

func (candidate Candidate) Normalized() Candidate {
	candidate.ReadyAt = candidate.ReadyAt.UTC()
	candidate.ReadyBucket = candidate.ReadyAt.UnixMilli() / ReadyBucketSize.Milliseconds()
	// PostgreSQL timestamps have microsecond precision. Keep the sortable value
	// as a fixed-width string so Lua/cjson never rounds or rewrites it.
	candidate.ReadyOrderKey = fmt.Sprintf("%020d", uint64(candidate.ReadyAt.UnixMicro())^(uint64(1)<<63))
	return candidate
}

func (candidate Candidate) Validate() error {
	if candidate.ProjectID == "" || candidate.RunID == "" || candidate.NodeRunID == "" || candidate.ExecutionNodeID == "" || candidate.StateVersion == 0 || candidate.ReadyAt.IsZero() || !candidate.ResourceClass.Valid() {
		return fmt.Errorf("ready candidate identity, version, time and resource class are required")
	}
	normalized := candidate.Normalized()
	if candidate.ReadyBucket != 0 && candidate.ReadyBucket != normalized.ReadyBucket {
		return fmt.Errorf("ready candidate bucket does not match ready_at")
	}
	if candidate.ReadyOrderKey != "" && candidate.ReadyOrderKey != normalized.ReadyOrderKey {
		return fmt.Errorf("ready candidate order key does not match ready_at")
	}
	return nil
}

// Inflight mirrors only the fields needed to reconstruct Project Load and
// Topic Queue Occupancy. Running attempts retain Project Load but no longer
// occupy the Kafka queue window.
type Inflight struct {
	AttemptID     string        `json:"attempt_id"`
	ProjectID     string        `json:"project_id"`
	ResourceClass ResourceClass `json:"resource_class"`
	QueueOccupied bool          `json:"queue_occupied"`
}

type AuthoritySnapshot struct {
	Candidates []Candidate
	Inflight   []Inflight
}

type DispatchCommand struct {
	Candidate Candidate
	AttemptID string
	TaskID    string
	TraceID   string
	Now       time.Time
}

type Task struct {
	MessageVersion  int
	TaskID          string
	ProjectID       string
	RunID           string
	NodeRunID       string
	ExecutionNodeID string
	AttemptID       string
	AttemptSequence uint32
	ResourceClass   ResourceClass
	OccurredAt      time.Time
	TraceID         string
}

type Authority interface {
	// candidateWindow bounds one reconciliation read. When more Ready rows
	// exist, subsequent reconciliations refill Redis after the oldest rows drain.
	LoadSchedulingSnapshot(context.Context, int) (AuthoritySnapshot, error)
	DispatchReady(context.Context, DispatchCommand) (Task, error)
}

type ReconcileLease struct {
	Owner        string
	Token        string
	FencingToken uint64
	ExpiresAt    time.Time
}

type Reservation struct {
	AttemptID     string        `json:"attempt_id"`
	ProjectID     string        `json:"project_id"`
	ResourceClass ResourceClass `json:"resource_class"`
	Candidate     Candidate     `json:"candidate"`
}

type TopicWindowPolicy struct {
	BufferDuration time.Duration
	SampleInterval time.Duration
	EWMAAlpha      float64
	Minimum        map[ResourceClass]int
	Maximum        map[ResourceClass]int
}

func (policy TopicWindowPolicy) Validate() error {
	if policy.BufferDuration <= 0 || policy.SampleInterval <= 0 || policy.EWMAAlpha <= 0 || policy.EWMAAlpha > 1 {
		return fmt.Errorf("topic queue window timing and EWMA alpha are invalid")
	}
	for _, class := range ResourceClasses() {
		minimum, minimumExists := policy.Minimum[class]
		maximum, maximumExists := policy.Maximum[class]
		if !minimumExists || !maximumExists || minimum <= 0 || maximum < minimum {
			return fmt.Errorf("topic queue window bounds for %s are invalid", class)
		}
	}
	return nil
}

type TopicState struct {
	Window    int
	Occupancy int
	EWMA      float64
}

type ReconcileResult struct {
	Generation     uint64
	CandidateCount int
	InflightCount  int
	Topics         map[ResourceClass]TopicState
}

type MemoryPolicy struct {
	HighWatermark   float64
	ResumeWatermark float64
}

func (policy MemoryPolicy) Validate() error {
	if policy.ResumeWatermark <= 0 || policy.HighWatermark <= policy.ResumeWatermark || policy.HighWatermark >= 1 {
		return fmt.Errorf("scheduling Redis memory watermarks are invalid")
	}
	return nil
}

// ReadyRegistrar is a post-commit acceleration port. Failure must never roll
// back an Engine transaction because PostgreSQL remains authoritative.
type ReadyRegistrar interface {
	RegisterReady(context.Context, []Candidate) error
}

// AttemptLifecycle closes Scheduling Redis accounting after authoritative
// Attempt transactions. Implementations must be idempotent.
type AttemptLifecycle interface {
	MarkClaimed(context.Context, string) error
	MarkTerminal(context.Context, string, bool) error
}

type CoordinationStore interface {
	ReadyRegistrar
	AttemptLifecycle
	AcquireReconcileLease(context.Context, string, time.Duration) (ReconcileLease, error)
	RefreshMemoryPressure(context.Context, MemoryPolicy) (bool, error)
	ListReservations(context.Context) ([]Reservation, error)
	Rebuild(context.Context, ReconcileLease, AuthoritySnapshot, TopicWindowPolicy) (ReconcileResult, error)
	CalibrateTopicWindows(context.Context, ReconcileLease, TopicWindowPolicy) (map[ResourceClass]TopicState, error)
	ReserveNext(context.Context, ResourceClass, string, time.Duration) (Reservation, bool, error)
	ConfirmReservation(context.Context, Reservation) error
	AbortReservation(context.Context, Reservation, bool) error
}

type WorkerRegistration struct {
	WorkerID      string
	ExecutorBuild string
	ResourceClass ResourceClass
	Slots         int
	Capabilities  []dsl.Coordinate
	TTL           time.Duration
}

func (registration WorkerRegistration) Validate() error {
	if registration.WorkerID == "" || registration.ExecutorBuild == "" || !registration.ResourceClass.Valid() || registration.Slots < 1 || registration.TTL <= 0 || len(registration.Capabilities) == 0 {
		return fmt.Errorf("worker registration identity, capabilities, slots and TTL are required")
	}
	actual := make(map[dsl.Coordinate]struct{}, len(registration.Capabilities))
	for _, coordinate := range registration.Capabilities {
		if coordinate.Type == "" || coordinate.Version == 0 {
			return fmt.Errorf("worker registration capability is invalid")
		}
		class, routable := BuiltinV1Router().Resolve(coordinate)
		if !routable || class != registration.ResourceClass {
			return fmt.Errorf("worker capability %s@%d does not belong to %s", coordinate.Type, coordinate.Version, registration.ResourceClass)
		}
		actual[coordinate] = struct{}{}
	}
	if len(actual) != len(registration.Capabilities) {
		return fmt.Errorf("worker capabilities contain duplicates")
	}
	required := RequiredCapabilities(registration.ResourceClass)
	if len(actual) != len(required) {
		return fmt.Errorf("worker must provide the complete %s capability set", registration.ResourceClass)
	}
	for _, coordinate := range required {
		if _, exists := actual[coordinate]; !exists {
			return fmt.Errorf("worker is missing required capability %s@%d", coordinate.Type, coordinate.Version)
		}
	}
	return nil
}

// Worker health/capability registration remains useful for operations and
// routing compatibility, but it is intentionally absent from Scheduler's
// Topic Queue Window calculation.
type Capacity struct{ Pools map[ResourceClass]int }

type CapacityProvider interface {
	HealthyCapacity(context.Context) (Capacity, error)
}

type CapacityRegistry interface {
	CapacityProvider
	RegisterWorker(context.Context, WorkerRegistration) error
}

type FixedCapacity Capacity

func (capacity FixedCapacity) HealthyCapacity(context.Context) (Capacity, error) {
	result := Capacity{Pools: make(map[ResourceClass]int, len(capacity.Pools))}
	for class, slots := range capacity.Pools {
		result.Pools[class] = slots
	}
	return result, nil
}

type Settings struct {
	CandidateBatch              int
	AdmissionConcurrency        int
	CapacityCalibrationInterval time.Duration
	ReadyReconcileInterval      time.Duration
	ReconcileLease              time.Duration
	ReservationTTL              time.Duration
	IdlePoll                    time.Duration
	IdlePollMax                 time.Duration
	TopicWindow                 TopicWindowPolicy
	Memory                      MemoryPolicy
}

func (settings Settings) Validate() error {
	if settings.CandidateBatch <= 0 || settings.AdmissionConcurrency <= 0 || settings.CapacityCalibrationInterval <= 0 || settings.ReadyReconcileInterval < settings.CapacityCalibrationInterval || settings.ReconcileLease <= 0 || settings.ReconcileLease >= settings.ReadyReconcileInterval || settings.ReservationTTL <= 0 || settings.IdlePoll <= 0 || settings.IdlePollMax < settings.IdlePoll {
		return fmt.Errorf("scheduler batching, timing or concurrency settings are invalid")
	}
	if settings.TopicWindow.SampleInterval != settings.CapacityCalibrationInterval {
		return fmt.Errorf("topic sample interval must equal scheduler capacity calibration interval")
	}
	if err := settings.TopicWindow.Validate(); err != nil {
		return err
	}
	return settings.Memory.Validate()
}

func sortCandidates(values []Candidate) {
	for index := range values {
		values[index] = values[index].Normalized()
	}
	sort.Slice(values, func(left, right int) bool {
		if values[left].ReadyBucket != values[right].ReadyBucket {
			return values[left].ReadyBucket < values[right].ReadyBucket
		}
		if values[left].ProjectID != values[right].ProjectID {
			return values[left].ProjectID < values[right].ProjectID
		}
		if values[left].Priority != values[right].Priority {
			return values[left].Priority > values[right].Priority
		}
		if values[left].ReadyOrderKey != values[right].ReadyOrderKey {
			return values[left].ReadyOrderKey < values[right].ReadyOrderKey
		}
		return values[left].NodeRunID < values[right].NodeRunID
	})
}
