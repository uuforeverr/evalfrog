// Package runtime owns Workflow Run, Node Run, and Node Attempt domain state.
//
// State fields are private by design: Engine and Attempt Coordinator
// must use explicit domain operations rather than updating status strings.
package runtime

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"time"
)

type RunPurpose string

const (
	RunPurposeTest       RunPurpose = "test"
	RunPurposeProduction RunPurpose = "production"
)

type DefinitionSource string

const (
	DefinitionDraftSnapshot    DefinitionSource = "draft_snapshot"
	DefinitionPublishedVersion DefinitionSource = "published_version"
)

type DefinitionReference struct {
	SnapshotID         string           `json:"snapshot_id"`
	DefinitionHash     string           `json:"definition_hash"`
	Source             DefinitionSource `json:"source"`
	PublishedVersionID string           `json:"published_version_id,omitempty"`
}

type CreateRunCommand struct {
	RunID         string
	ProjectID     string
	WorkflowID    string
	Purpose       RunPurpose
	Definition    DefinitionReference
	WorkflowInput json.RawMessage
	DeadlineAt    time.Time
	CreatedAt     time.Time
}

type RunState string

const (
	RunPending   RunState = "pending"
	RunRunning   RunState = "running"
	RunSucceeded RunState = "succeeded"
	RunFailed    RunState = "failed"
	RunCanceled  RunState = "canceled"
	RunTimedOut  RunState = "timed_out"
)

func (state RunState) Terminal() bool {
	switch state {
	case RunSucceeded, RunFailed, RunCanceled, RunTimedOut:
		return true
	default:
		return false
	}
}

type TerminationKind string

const (
	TerminationFailed   TerminationKind = "failed"
	TerminationCanceled TerminationKind = "canceled"
	TerminationTimedOut TerminationKind = "timed_out"
)

type TerminationIntent struct {
	Kind        TerminationKind `json:"kind"`
	RequestedAt time.Time       `json:"requested_at"`
	Cause       Failure         `json:"cause"`
}

type Failure struct {
	Code            string         `json:"code"`
	Phase           string         `json:"phase"`
	Retryable       bool           `json:"retryable"`
	RunID           string         `json:"run_id"`
	SnapshotID      string         `json:"snapshot_id"`
	DefinitionHash  string         `json:"definition_hash"`
	ExecutionNodeID string         `json:"execution_node_id,omitempty"`
	DSLField        string         `json:"dsl_field,omitempty"`
	ExecutionEdgeID string         `json:"execution_edge_id,omitempty"`
	AttemptID       string         `json:"attempt_id,omitempty"`
	Expected        string         `json:"expected,omitempty"`
	Actual          string         `json:"actual,omitempty"`
	Message         string         `json:"message"`
	Details         map[string]any `json:"details,omitempty"`
}

type WorkflowRun struct {
	id               string
	projectID        string
	workflowID       string
	purpose          RunPurpose
	definition       DefinitionReference
	workflowInput    json.RawMessage
	workflowOutput   json.RawMessage
	deadlineAt       time.Time
	createdAt        time.Time
	state            RunState
	stateVersion     uint64
	nodeRunCount     uint32
	executionNodeIDs map[string]struct{}
	termination      *TerminationIntent
}

func NewWorkflowRun(command CreateRunCommand) (*WorkflowRun, error) {
	if command.RunID == "" || command.ProjectID == "" || command.WorkflowID == "" ||
		command.Definition.SnapshotID == "" || command.Definition.DefinitionHash == "" ||
		command.CreatedAt.IsZero() || command.DeadlineAt.IsZero() || !command.DeadlineAt.After(command.CreatedAt) {
		return nil, fmt.Errorf("%w: identity, immutable definition, created_at and future deadline are required", ErrInvalidRun)
	}
	if command.Purpose != RunPurposeTest && command.Purpose != RunPurposeProduction {
		return nil, fmt.Errorf("%w: run purpose is invalid", ErrInvalidRun)
	}
	if command.Definition.Source != DefinitionDraftSnapshot && command.Definition.Source != DefinitionPublishedVersion {
		return nil, fmt.Errorf("%w: definition source is invalid", ErrInvalidRun)
	}
	if command.Purpose == RunPurposeProduction && command.Definition.Source != DefinitionPublishedVersion {
		return nil, fmt.Errorf("%w: production runs require a published version", ErrInvalidRun)
	}
	if command.Definition.Source == DefinitionPublishedVersion && command.Definition.PublishedVersionID == "" {
		return nil, fmt.Errorf("%w: published definition source requires version identity", ErrInvalidRun)
	}
	if command.Definition.Source == DefinitionDraftSnapshot && command.Definition.PublishedVersionID != "" {
		return nil, fmt.Errorf("%w: draft definition source cannot carry a published version identity", ErrInvalidRun)
	}
	input, err := cloneJSONObject(command.WorkflowInput)
	if err != nil {
		return nil, fmt.Errorf("%w: workflow input: %v", ErrInvalidRun, err)
	}
	return &WorkflowRun{
		id: command.RunID, projectID: command.ProjectID, workflowID: command.WorkflowID,
		purpose: command.Purpose, definition: command.Definition, workflowInput: input,
		deadlineAt: command.DeadlineAt, createdAt: command.CreatedAt, state: RunPending, stateVersion: 1,
	}, nil
}

func (run *WorkflowRun) ID() string                      { return run.id }
func (run *WorkflowRun) ProjectID() string               { return run.projectID }
func (run *WorkflowRun) WorkflowID() string              { return run.workflowID }
func (run *WorkflowRun) Purpose() RunPurpose             { return run.purpose }
func (run *WorkflowRun) Definition() DefinitionReference { return run.definition }
func (run *WorkflowRun) State() RunState                 { return run.state }
func (run *WorkflowRun) StateVersion() uint64            { return run.stateVersion }
func (run *WorkflowRun) NodeRunCount() uint32            { return run.nodeRunCount }
func (run *WorkflowRun) DeadlineAt() time.Time           { return run.deadlineAt }
func (run *WorkflowRun) CreatedAt() time.Time            { return run.createdAt }
func (run *WorkflowRun) WorkflowInput() json.RawMessage  { return cloneRaw(run.workflowInput) }
func (run *WorkflowRun) WorkflowOutput() json.RawMessage { return cloneRaw(run.workflowOutput) }
func (run *WorkflowRun) Termination() (TerminationIntent, bool) {
	if run.termination == nil {
		return TerminationIntent{}, false
	}
	return *run.termination, true
}

func (run *WorkflowRun) Start(executionNodeIDs []string) error {
	if len(executionNodeIDs) < 2 {
		return fmt.Errorf("a workflow run requires at least start and end node runs")
	}
	identities := make(map[string]struct{}, len(executionNodeIDs))
	for _, id := range executionNodeIDs {
		if id == "" {
			return fmt.Errorf("execution node identity is required")
		}
		if _, duplicate := identities[id]; duplicate {
			return fmt.Errorf("execution node identity %q is duplicated", id)
		}
		identities[id] = struct{}{}
	}
	if err := run.transition(RunRunning); err != nil {
		return err
	}
	run.nodeRunCount = uint32(len(executionNodeIDs))
	run.executionNodeIDs = identities
	return nil
}

// FailInitialization is the only legal pending -> failed path. It records a
// deterministic compatibility/DSL failure before any Node Run is exposed.
func (run *WorkflowRun) FailInitialization(failure Failure, at time.Time) error {
	if run.state != RunPending || run.termination != nil || failure.Code == "" || at.IsZero() || at.Before(run.createdAt) {
		return fmt.Errorf("run initialization failure is invalid")
	}
	run.termination = &TerminationIntent{Kind: TerminationFailed, RequestedAt: at, Cause: failure}
	run.state = RunFailed
	run.stateVersion++
	return nil
}

// RequestTermination is first-writer-wins. Replays and later competing intents
// do not overwrite the original outcome and return applied=false.
func (run *WorkflowRun) RequestTermination(intent TerminationIntent) (bool, error) {
	if run.state.Terminal() {
		return false, ErrTerminationFinal
	}
	if intent.RequestedAt.IsZero() || intent.Cause.Code == "" {
		return false, fmt.Errorf("termination intent requires timestamp and cause")
	}
	if intent.RequestedAt.Before(run.createdAt) {
		return false, fmt.Errorf("termination intent cannot predate the run")
	}
	switch intent.Kind {
	case TerminationFailed, TerminationCanceled, TerminationTimedOut:
	default:
		return false, fmt.Errorf("termination kind %q is invalid", intent.Kind)
	}
	// A Run may be created durably but wait behind a delayed/reordered
	// RunCreated event. Cancellation and the immutable workflow deadline are
	// both meaningful before Node Runs are initialized; a business failure is
	// not, because no Node has executed yet.
	if run.state == RunPending && intent.Kind == TerminationFailed {
		return false, fmt.Errorf("pending run only accepts cancellation or deadline timeout")
	}
	if run.state == RunPending && intent.Kind == TerminationTimedOut && intent.RequestedAt.Before(run.deadlineAt) {
		return false, fmt.Errorf("pending run timeout cannot predate its deadline")
	}
	if run.termination != nil {
		return false, nil
	}
	copy := intent
	run.termination = &copy
	run.stateVersion++
	return true, nil
}

func (run *WorkflowRun) CompleteTermination(nodes []*NodeRun) error {
	if run.termination == nil {
		return fmt.Errorf("termination intent is required")
	}
	if err := run.validateNodeSet(nodes); err != nil {
		return err
	}
	for _, node := range nodes {
		if !node.state.Terminal() {
			return fmt.Errorf("all node runs must be terminal before run termination")
		}
	}
	var target RunState
	switch run.termination.Kind {
	case TerminationFailed:
		target = RunFailed
	case TerminationCanceled:
		target = RunCanceled
	case TerminationTimedOut:
		target = RunTimedOut
	default:
		return fmt.Errorf("termination kind %q is invalid", run.termination.Kind)
	}
	return run.transition(target)
}

func (run *WorkflowRun) transition(target RunState) error {
	if !validRunTransition(run.state, target) {
		return &TransitionError{Entity: "workflow_run", From: string(run.state), To: string(target)}
	}
	run.state = target
	run.stateVersion++
	return nil
}

type NodeKind string

const (
	NodeControl NodeKind = "control"
	NodeTask    NodeKind = "task"
)

type NodeState string

const (
	NodePending   NodeState = "pending"
	NodeReady     NodeState = "ready"
	NodeQueued    NodeState = "queued"
	NodeRunning   NodeState = "running"
	NodeRetryWait NodeState = "retry_wait"
	NodeSucceeded NodeState = "succeeded"
	NodeFailed    NodeState = "failed"
	NodeTimedOut  NodeState = "timed_out"
	NodeSkipped   NodeState = "skipped"
	NodeCanceled  NodeState = "canceled"
)

func (state NodeState) Terminal() bool {
	switch state {
	case NodeSucceeded, NodeFailed, NodeTimedOut, NodeSkipped, NodeCanceled:
		return true
	default:
		return false
	}
}

type RetryKind string

const (
	AttemptInitial  RetryKind = "initial"
	AttemptBusiness RetryKind = "business_retry"
	AttemptRecovery RetryKind = "recovery"
)

type NodeRun struct {
	runID                string
	executionNodeID      string
	kind                 NodeKind
	state                NodeState
	stateVersion         uint64
	activated            bool
	selectedRoute        string
	resolvedInputs       map[string]json.RawMessage
	effectiveOutputs     map[string]json.RawMessage
	effectiveAttemptID   string
	currentAttemptID     string
	nextAttemptSeq       uint32
	businessAttemptCount uint32
	recoveryCount        uint32
	nextAttemptKind      RetryKind
	nextRetryAt          time.Time
	failure              *Failure
	cancelReason         string
}

func NewNodeRun(runID, executionNodeID string, kind NodeKind) (*NodeRun, error) {
	if runID == "" || executionNodeID == "" || (kind != NodeControl && kind != NodeTask) {
		return nil, fmt.Errorf("invalid node run identity or kind")
	}
	return &NodeRun{runID: runID, executionNodeID: executionNodeID, kind: kind, state: NodePending, stateVersion: 1, nextAttemptKind: AttemptInitial}, nil
}

func (node *NodeRun) RunID() string                { return node.runID }
func (node *NodeRun) ExecutionNodeID() string      { return node.executionNodeID }
func (node *NodeRun) Kind() NodeKind               { return node.kind }
func (node *NodeRun) State() NodeState             { return node.state }
func (node *NodeRun) StateVersion() uint64         { return node.stateVersion }
func (node *NodeRun) Activated() bool              { return node.activated }
func (node *NodeRun) SelectedRoute() string        { return node.selectedRoute }
func (node *NodeRun) EffectiveAttemptID() string   { return node.effectiveAttemptID }
func (node *NodeRun) CurrentAttemptID() string     { return node.currentAttemptID }
func (node *NodeRun) BusinessAttemptCount() uint32 { return node.businessAttemptCount }
func (node *NodeRun) RecoveryCount() uint32        { return node.recoveryCount }
func (node *NodeRun) NextAttemptKind() RetryKind   { return node.nextAttemptKind }
func (node *NodeRun) NextRetryAt() time.Time       { return node.nextRetryAt }
func (node *NodeRun) ResolvedInputs() map[string]json.RawMessage {
	return cloneValues(node.resolvedInputs)
}
func (node *NodeRun) EffectiveOutputs() map[string]json.RawMessage {
	return cloneValues(node.effectiveOutputs)
}
func (node *NodeRun) Failure() (Failure, bool) {
	if node.failure == nil {
		return Failure{}, false
	}
	return *node.failure, true
}
func (node *NodeRun) CancelReason() string { return node.cancelReason }

func (node *NodeRun) ExecutionIdempotencyKey() string {
	return node.runID + ":" + node.executionNodeID
}

func (node *NodeRun) Activate() error {
	if node.state != NodePending {
		return fmt.Errorf("only pending nodes can be activated")
	}
	if !node.activated {
		node.activated = true
		node.stateVersion++
	}
	return nil
}

func (node *NodeRun) Ready(inputs map[string]json.RawMessage) error {
	if node.kind != NodeTask || !node.activated {
		return fmt.Errorf("only activated task nodes can become ready")
	}
	if err := node.transition(NodeReady); err != nil {
		return err
	}
	node.resolvedInputs = cloneValues(inputs)
	return nil
}

func (node *NodeRun) Skip() error {
	if node.activated {
		return ErrActivatedNodeSkipped
	}
	return node.transition(NodeSkipped)
}

func (node *NodeRun) SucceedControl(route string, outputs map[string]json.RawMessage) error {
	if node.kind != NodeControl || !node.activated {
		return fmt.Errorf("only activated control nodes can succeed")
	}
	if err := node.transition(NodeSucceeded); err != nil {
		return err
	}
	node.selectedRoute = route
	node.effectiveOutputs = cloneValues(outputs)
	return nil
}

func (node *NodeRun) FailControl(failure Failure) error {
	if node.kind != NodeControl || failure.Code == "" {
		return fmt.Errorf("control failure is invalid")
	}
	if err := node.transition(NodeFailed); err != nil {
		return err
	}
	copy := failure
	node.failure = &copy
	return nil
}

func (node *NodeRun) FailBeforeAttempt(failure Failure) error {
	if node.kind != NodeTask || !node.activated || node.currentAttemptID != "" || failure.Code == "" {
		return fmt.Errorf("pre-attempt task failure is invalid")
	}
	if err := node.transition(NodeFailed); err != nil {
		return err
	}
	copy := failure
	node.failure = &copy
	return nil
}

func (node *NodeRun) QueueAttempt(attemptID string) (uint32, RetryKind, error) {
	if node.kind != NodeTask {
		return 0, "", ErrControlAttempt
	}
	if attemptID == "" {
		return 0, "", fmt.Errorf("attempt id is required")
	}
	if err := node.transition(NodeQueued); err != nil {
		return 0, "", err
	}
	node.nextAttemptSeq++
	kind := node.nextAttemptKind
	if kind == AttemptRecovery {
		node.recoveryCount++
	} else {
		node.businessAttemptCount++
	}
	node.currentAttemptID = attemptID
	node.nextRetryAt = time.Time{}
	return node.nextAttemptSeq, kind, nil
}

func (node *NodeRun) AttemptStarted(attemptID string) error {
	if node.currentAttemptID != attemptID {
		return fmt.Errorf("attempt is not current")
	}
	return node.transition(NodeRunning)
}

func (node *NodeRun) RetryWait(attemptID string, next RetryKind, dueAt time.Time) error {
	if node.currentAttemptID != attemptID || (next != AttemptBusiness && next != AttemptRecovery) || dueAt.IsZero() {
		return fmt.Errorf("retry decision is invalid")
	}
	if err := node.transition(NodeRetryWait); err != nil {
		return err
	}
	node.nextAttemptKind = next
	node.nextRetryAt = dueAt
	return nil
}

func (node *NodeRun) RetryDue(at time.Time) (bool, error) {
	if node.state != NodeRetryWait || at.IsZero() || at.Before(node.nextRetryAt) {
		return false, nil
	}
	if err := node.transition(NodeReady); err != nil {
		return false, err
	}
	node.nextRetryAt = time.Time{}
	return true, nil
}

func (node *NodeRun) SucceedAttempt(attemptID string, outputs map[string]json.RawMessage) error {
	if node.kind != NodeTask || node.currentAttemptID != attemptID {
		return fmt.Errorf("successful attempt is not current")
	}
	if err := node.transition(NodeSucceeded); err != nil {
		return err
	}
	node.effectiveAttemptID = attemptID
	node.effectiveOutputs = cloneValues(outputs)
	return nil
}

func (node *NodeRun) FailAttempt(attemptID string, target NodeState, failure Failure) error {
	if node.kind != NodeTask || node.currentAttemptID != attemptID || failure.Code == "" {
		return fmt.Errorf("attempt failure is invalid")
	}
	if target != NodeFailed && target != NodeTimedOut {
		return fmt.Errorf("attempt failure target is invalid")
	}
	if err := node.transition(target); err != nil {
		return err
	}
	copy := failure
	node.failure = &copy
	return nil
}

func (node *NodeRun) Cancel(reason string) error {
	if reason == "" {
		return fmt.Errorf("cancel reason is required")
	}
	if err := node.transition(NodeCanceled); err != nil {
		return err
	}
	node.cancelReason = reason
	return nil
}

func (node *NodeRun) transition(target NodeState) error {
	if !validNodeTransition(node.kind, node.state, target) {
		return &TransitionError{Entity: "node_run", From: string(node.state), To: string(target)}
	}
	node.state = target
	node.stateVersion++
	return nil
}

type AttemptState string

const (
	AttemptQueued    AttemptState = "queued"
	AttemptRunning   AttemptState = "running"
	AttemptSucceeded AttemptState = "succeeded"
	AttemptFailed    AttemptState = "failed"
	AttemptTimedOut  AttemptState = "timed_out"
	AttemptCanceled  AttemptState = "canceled"
	AttemptLost      AttemptState = "lost"
)

func (state AttemptState) Terminal() bool {
	switch state {
	case AttemptSucceeded, AttemptFailed, AttemptTimedOut, AttemptCanceled, AttemptLost:
		return true
	default:
		return false
	}
}

type AttemptResult struct {
	State        AttemptState               `json:"state"`
	Outputs      map[string]json.RawMessage `json:"outputs,omitempty"`
	ErrorCode    string                     `json:"error_code,omitempty"`
	Message      string                     `json:"message,omitempty"`
	DSLField     string                     `json:"dsl_field,omitempty"`
	ErrorDetails map[string]any             `json:"error_details,omitempty"`
}

func (result AttemptResult) Equal(other AttemptResult) bool {
	if result.State != other.State || result.ErrorCode != other.ErrorCode || result.Message != other.Message || result.DSLField != other.DSLField || len(result.Outputs) != len(other.Outputs) || !reflect.DeepEqual(result.ErrorDetails, other.ErrorDetails) {
		return false
	}
	for key, value := range result.Outputs {
		candidate, exists := other.Outputs[key]
		if !exists || !bytes.Equal(value, candidate) {
			return false
		}
	}
	return true
}

type NodeAttempt struct {
	id           string
	nodeRunID    string
	sequence     uint32
	kind         RetryKind
	state        AttemptState
	stateVersion uint64
	result       *AttemptResult
}

func NewNodeAttempt(id, nodeRunID string, sequence uint32, kind RetryKind) (*NodeAttempt, error) {
	if id == "" || nodeRunID == "" || sequence == 0 || (kind != AttemptInitial && kind != AttemptBusiness && kind != AttemptRecovery) {
		return nil, fmt.Errorf("invalid attempt identity")
	}
	return &NodeAttempt{id: id, nodeRunID: nodeRunID, sequence: sequence, kind: kind, state: AttemptQueued, stateVersion: 1}, nil
}

func (attempt *NodeAttempt) ID() string           { return attempt.id }
func (attempt *NodeAttempt) NodeRunID() string    { return attempt.nodeRunID }
func (attempt *NodeAttempt) Sequence() uint32     { return attempt.sequence }
func (attempt *NodeAttempt) Kind() RetryKind      { return attempt.kind }
func (attempt *NodeAttempt) State() AttemptState  { return attempt.state }
func (attempt *NodeAttempt) StateVersion() uint64 { return attempt.stateVersion }
func (attempt *NodeAttempt) Result() (AttemptResult, bool) {
	if attempt.result == nil {
		return AttemptResult{}, false
	}
	return cloneResult(*attempt.result), true
}

func (attempt *NodeAttempt) Start() error { return attempt.transition(AttemptRunning) }

func (attempt *NodeAttempt) Complete(result AttemptResult) error {
	if attempt.state.Terminal() {
		if attempt.result != nil && attempt.result.Equal(result) {
			return nil
		}
		return &TransitionError{Entity: "node_attempt", From: string(attempt.state), To: string(result.State)}
	}
	if !result.State.Terminal() {
		return fmt.Errorf("attempt result must be terminal")
	}
	if result.State == AttemptSucceeded && result.ErrorCode != "" {
		return fmt.Errorf("successful attempt cannot contain an error code")
	}
	if result.State != AttemptSucceeded && result.ErrorCode == "" {
		return fmt.Errorf("unsuccessful attempt requires an error code")
	}
	if err := attempt.transition(result.State); err != nil {
		return err
	}
	copy := cloneResult(result)
	attempt.result = &copy
	return nil
}

func (attempt *NodeAttempt) transition(target AttemptState) error {
	if !validAttemptTransition(attempt.state, target) {
		return &TransitionError{Entity: "node_attempt", From: string(attempt.state), To: string(target)}
	}
	attempt.state = target
	attempt.stateVersion++
	return nil
}

// CompleteWorkflowSuccess models the single atomic write boundary that M5 will
// implement as one PostgreSQL transaction.
func CompleteWorkflowSuccess(run *WorkflowRun, end *NodeRun, nodes []*NodeRun, output json.RawMessage) error {
	if run == nil || end == nil || run.state != RunRunning || run.termination != nil || end.kind != NodeControl || !end.activated || end.state != NodePending {
		return fmt.Errorf("workflow success preconditions are not met")
	}
	if err := run.validateNodeSet(nodes); err != nil {
		return err
	}
	endPresent := false
	for _, node := range nodes {
		if node.executionNodeID == end.executionNodeID {
			endPresent = true
			continue
		}
		if node.state != NodeSucceeded && node.state != NodeSkipped {
			return fmt.Errorf("all non-end nodes must be succeeded or skipped")
		}
	}
	if !endPresent {
		return fmt.Errorf("workflow success node set does not contain end")
	}
	object, err := cloneJSONObject(output)
	if err != nil {
		return err
	}
	if !validNodeTransition(end.kind, end.state, NodeSucceeded) || !validRunTransition(run.state, RunSucceeded) {
		return ErrInvalidTransition
	}
	end.state = NodeSucceeded
	end.stateVersion++
	run.workflowOutput = object
	run.state = RunSucceeded
	run.stateVersion++
	return nil
}

func (run *WorkflowRun) validateNodeSet(nodes []*NodeRun) error {
	expected := run.nodeRunCount
	if run.state == RunPending && run.termination != nil && (run.termination.Kind == TerminationCanceled || run.termination.Kind == TerminationTimedOut) {
		expected = 0
	}
	if uint32(len(nodes)) != expected {
		return fmt.Errorf("node run set size %d does not match initialized size %d", len(nodes), expected)
	}
	identities := make(map[string]struct{}, len(nodes))
	for _, node := range nodes {
		if node == nil || node.runID != run.id || node.executionNodeID == "" {
			return fmt.Errorf("node run set contains an invalid identity")
		}
		if _, duplicate := identities[node.executionNodeID]; duplicate {
			return fmt.Errorf("node run set contains a duplicate execution node")
		}
		if expected > 0 {
			if _, exists := run.executionNodeIDs[node.executionNodeID]; !exists {
				return fmt.Errorf("node run set contains unexpected execution node %q", node.executionNodeID)
			}
		}
		identities[node.executionNodeID] = struct{}{}
	}
	return nil
}

func cloneJSONObject(raw json.RawMessage) (json.RawMessage, error) {
	if !json.Valid(raw) {
		return nil, fmt.Errorf("value is not valid JSON")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	if _, ok := value.(map[string]any); !ok {
		return nil, fmt.Errorf("value must be a JSON object")
	}
	return cloneRaw(raw), nil
}

func cloneValues(values map[string]json.RawMessage) map[string]json.RawMessage {
	if values == nil {
		return nil
	}
	copy := make(map[string]json.RawMessage, len(values))
	for key, value := range values {
		copy[key] = cloneRaw(value)
	}
	return copy
}

func cloneRaw(value json.RawMessage) json.RawMessage { return append(json.RawMessage(nil), value...) }

func cloneResult(result AttemptResult) AttemptResult {
	result.Outputs = cloneValues(result.Outputs)
	if result.ErrorDetails != nil {
		result.ErrorDetails = maps.Clone(result.ErrorDetails)
	}
	return result
}

func IsInvalidTransition(err error) bool { return errors.Is(err, ErrInvalidTransition) }
