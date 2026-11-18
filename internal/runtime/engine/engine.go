// Package engine interprets immutable DSL snapshots and is the sole owner of
// Workflow Run and Node Run semantic progression.
package engine

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/big"
	"sort"
	"time"

	"github.com/uu999/evalfrog/internal/dsl"
	"github.com/uu999/evalfrog/internal/runtime"
)

const (
	FailureNodeFailed             = "NODE_FAILED"
	FailureNodeTimeout            = "NODE_TIMEOUT"
	FailureRunCanceled            = "RUN_CANCELED"
	FailureRunTimedOut            = "RUN_TIMED_OUT"
	FailureOutputContract         = "RUNTIME_OUTPUT_CONTRACT_VIOLATION"
	FailureInputContract          = "RUNTIME_INPUT_CONTRACT_VIOLATION"
	FailureBranchPathNotFound     = "BRANCH_PATH_NOT_FOUND"
	FailureBranchOperatorMismatch = "BRANCH_OPERATOR_TYPE_MISMATCH"
)

type Snapshot struct {
	ID             string
	DefinitionHash string
	DSL            dsl.Document
}

type EdgeState string

const (
	EdgePending  EdgeState = "pending"
	EdgeActive   EdgeState = "active"
	EdgeInactive EdgeState = "inactive"
)

type Engine struct {
	snapshot     Snapshot
	run          *runtime.WorkflowRun
	doc          dsl.Document
	nodeDefs     map[dsl.NodeID]dsl.Node
	nodes        map[dsl.NodeID]*runtime.NodeRun
	attempts     map[string]*runtime.NodeAttempt
	attemptNodes map[string]dsl.NodeID
	edges        map[dsl.EdgeID]EdgeState
	incoming     map[dsl.NodeID][]dsl.Edge
	outgoing     map[dsl.NodeID][]dsl.Edge
}

func NewBuiltinV1(snapshot Snapshot, command runtime.CreateRunCommand) (*Engine, error) {
	return New(snapshot, command, dsl.BuiltinV2Contract(), dsl.BuiltinV2Compatibility())
}

func New(snapshot Snapshot, command runtime.CreateRunCommand, contract dsl.Contract, compatibility dsl.CompatibilityChecker) (*Engine, error) {
	if snapshot.ID == "" || snapshot.DefinitionHash == "" || command.Definition.SnapshotID != snapshot.ID || command.Definition.DefinitionHash != snapshot.DefinitionHash {
		return nil, &Error{Code: "RUNTIME_SNAPSHOT_BINDING_INVALID"}
	}
	document, err := cloneDocument(snapshot.DSL)
	if err != nil {
		return nil, &Error{Code: "RUNTIME_DSL_INVALID", Cause: err}
	}
	snapshot.DSL = document
	if issues := compatibility.CheckAll(snapshot.DSL); len(issues) > 0 {
		return nil, &Error{Code: issues[0].Code, NodeID: string(issues[0].NodeID), Field: issues[0].Field}
	}
	if issues := contract.Validate(snapshot.DSL); len(issues) > 0 {
		return nil, &Error{Code: "RUNTIME_DSL_INVALID", NodeID: string(issues[0].NodeID), Field: issues[0].Field}
	}
	run, err := runtime.NewWorkflowRun(command)
	if err != nil {
		return nil, err
	}
	instance := &Engine{
		snapshot: snapshot, run: run, doc: snapshot.DSL,
		nodeDefs: make(map[dsl.NodeID]dsl.Node, len(snapshot.DSL.Nodes)),
		nodes:    make(map[dsl.NodeID]*runtime.NodeRun, len(snapshot.DSL.Nodes)),
		attempts: make(map[string]*runtime.NodeAttempt), attemptNodes: make(map[string]dsl.NodeID), edges: make(map[dsl.EdgeID]EdgeState, len(snapshot.DSL.Edges)),
		incoming: make(map[dsl.NodeID][]dsl.Edge), outgoing: make(map[dsl.NodeID][]dsl.Edge),
	}
	for _, definition := range snapshot.DSL.Nodes {
		kind := runtime.NodeTask
		if definition.Kind == dsl.KindControl {
			kind = runtime.NodeControl
		}
		node, nodeErr := runtime.NewNodeRun(command.RunID, string(definition.ID), kind)
		if nodeErr != nil {
			return nil, nodeErr
		}
		instance.nodeDefs[definition.ID] = definition
		instance.nodes[definition.ID] = node
	}
	for _, edge := range snapshot.DSL.Edges {
		instance.edges[edge.ID] = EdgePending
		instance.incoming[edge.TargetNodeID] = append(instance.incoming[edge.TargetNodeID], edge)
		instance.outgoing[edge.SourceNodeID] = append(instance.outgoing[edge.SourceNodeID], edge)
	}
	for id := range instance.incoming {
		sort.Slice(instance.incoming[id], func(i, j int) bool { return instance.incoming[id][i].ID < instance.incoming[id][j].ID })
	}
	for id := range instance.outgoing {
		sort.Slice(instance.outgoing[id], func(i, j int) bool { return instance.outgoing[id][i].ID < instance.outgoing[id][j].ID })
	}
	if err := instance.initialize(); err != nil {
		return nil, err
	}
	return instance, nil
}

func (engine *Engine) Run() *runtime.WorkflowRun {
	copy := *engine.run
	return &copy
}

func (engine *Engine) Node(id dsl.NodeID) (*runtime.NodeRun, bool) {
	node, exists := engine.nodes[id]
	if !exists {
		return nil, false
	}
	copy := *node
	return &copy, true
}

func (engine *Engine) Attempt(id string) (*runtime.NodeAttempt, bool) {
	attempt, exists := engine.attempts[id]
	if !exists {
		return nil, false
	}
	copy := *attempt
	return &copy, true
}

func (engine *Engine) EdgeState(id dsl.EdgeID) (EdgeState, bool) {
	state, exists := engine.edges[id]
	return state, exists
}

func (engine *Engine) ReadyNodeIDs() []dsl.NodeID {
	ids := make([]dsl.NodeID, 0)
	if _, terminating := engine.run.Termination(); terminating {
		return ids
	}
	for id, node := range engine.nodes {
		if node.State() == runtime.NodeReady {
			ids = append(ids, id)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

func (engine *Engine) QueueNode(nodeID dsl.NodeID, attemptID string) (*runtime.NodeAttempt, error) {
	if _, terminating := engine.run.Termination(); terminating {
		return nil, fmt.Errorf("run is terminating")
	}
	if _, exists := engine.attempts[attemptID]; exists {
		return nil, fmt.Errorf("attempt id already exists")
	}
	node, exists := engine.nodes[nodeID]
	if !exists {
		return nil, fmt.Errorf("node does not exist")
	}
	sequence, kind, err := node.QueueAttempt(attemptID)
	if err != nil {
		return nil, err
	}
	attempt, err := runtime.NewNodeAttempt(attemptID, node.RunID()+":"+node.ExecutionNodeID(), sequence, kind)
	if err != nil {
		return nil, err
	}
	engine.attempts[attemptID] = attempt
	engine.attemptNodes[attemptID] = nodeID
	copy := *attempt
	return &copy, nil
}

func (engine *Engine) StartAttempt(attemptID string) error {
	attempt, exists := engine.attempts[attemptID]
	if !exists {
		return fmt.Errorf("attempt does not exist")
	}
	if attempt.State() == runtime.AttemptRunning {
		return nil
	}
	if err := attempt.Start(); err != nil {
		return err
	}
	node := engine.nodes[engine.attemptNodes[attemptID]]
	return node.AttemptStarted(attemptID)
}

// RecordAttemptResult models the Attempt Coordinator transaction. It only
// persists a terminal physical fact and Output Candidate; it does not advance
// Node Run semantics or expose Effective Output.
func (engine *Engine) RecordAttemptResult(attemptID string, result runtime.AttemptResult) (bool, error) {
	attempt, exists := engine.attempts[attemptID]
	if !exists {
		return false, fmt.Errorf("attempt does not exist")
	}
	node := engine.nodes[engine.attemptNodes[attemptID]]
	if node == nil {
		return false, fmt.Errorf("attempt node does not exist")
	}
	if node.CurrentAttemptID() != attemptID {
		if recorded, exists := attempt.Result(); exists && recorded.Equal(result) {
			return false, nil
		}
		return false, nil
	}
	if recorded, exists := attempt.Result(); exists {
		if recorded.Equal(result) {
			return false, nil
		}
		return false, fmt.Errorf("attempt result conflicts with recorded terminal fact")
	}
	if err := attempt.Complete(result); err != nil {
		return false, err
	}
	return true, nil
}

// HandleAttemptCompleted models the Engine Inbox consumer. It reads the
// already-recorded Attempt fact and decides Node Run/Retry/Termination state.
func (engine *Engine) HandleAttemptCompleted(attemptID string, at time.Time) error {
	attempt, exists := engine.attempts[attemptID]
	if !exists {
		return fmt.Errorf("attempt does not exist")
	}
	if at.IsZero() {
		return fmt.Errorf("completion timestamp is required")
	}
	if at.Before(engine.run.CreatedAt()) {
		return fmt.Errorf("completion timestamp cannot predate the run")
	}
	result, exists := attempt.Result()
	if !exists {
		return fmt.Errorf("attempt result has not been recorded")
	}
	node := engine.nodes[engine.attemptNodes[attemptID]]
	if node == nil {
		return fmt.Errorf("attempt node does not exist")
	}
	if node.CurrentAttemptID() != attemptID {
		return nil
	}
	if node.State().Terminal() || node.State() == runtime.NodeRetryWait || node.State() == runtime.NodeReady {
		return nil
	}
	if intent, terminating := engine.run.Termination(); terminating {
		if result.State == runtime.AttemptSucceeded {
			if err := engine.acceptSuccess(node, engine.nodeDefs[dsl.NodeID(node.ExecutionNodeID())], attemptID, result.Outputs); err != nil {
				if cancelErr := node.Cancel(cancelReason(intent.Kind)); cancelErr != nil {
					return cancelErr
				}
			}
		} else if err := node.Cancel(cancelReason(intent.Kind)); err != nil {
			return err
		}
		return engine.drive(at)
	}
	definition := engine.nodeDefs[dsl.NodeID(node.ExecutionNodeID())]
	switch result.State {
	case runtime.AttemptSucceeded:
		if err := engine.acceptSuccess(node, definition, attemptID, result.Outputs); err != nil {
			failure := engine.failure(FailureOutputContract, "output_binding", err.Error(), dsl.NodeID(node.ExecutionNodeID()), attemptID, "outputs", false)
			if transitionErr := node.FailAttempt(attemptID, runtime.NodeFailed, failure); transitionErr != nil {
				return transitionErr
			}
			_, _ = engine.run.RequestTermination(runtime.TerminationIntent{Kind: runtime.TerminationFailed, RequestedAt: at, Cause: failure})
		}
	case runtime.AttemptLost:
		if node.RecoveryCount() < definition.ExecutionPolicy.MaxRecoveries {
			if err := node.RetryWait(attemptID, runtime.AttemptRecovery, retryDueAt(at, definition.ExecutionPolicy)); err != nil {
				return err
			}
			return nil
		}
		failure := engine.failure(result.ErrorCode, "node_execution", result.Message, dsl.NodeID(node.ExecutionNodeID()), attemptID, result.DSLField, false)
		failure.Details = cloneDetails(result.ErrorDetails)
		if err := node.FailAttempt(attemptID, runtime.NodeFailed, failure); err != nil {
			return err
		}
		_, _ = engine.run.RequestTermination(runtime.TerminationIntent{Kind: runtime.TerminationFailed, RequestedAt: at, Cause: failure})
	case runtime.AttemptFailed, runtime.AttemptTimedOut:
		if retryable(definition.ExecutionPolicy, result.ErrorCode) && node.BusinessAttemptCount() < definition.ExecutionPolicy.MaxAttempts {
			if err := node.RetryWait(attemptID, runtime.AttemptBusiness, retryDueAt(at, definition.ExecutionPolicy)); err != nil {
				return err
			}
			return nil
		}
		failureCode := result.ErrorCode
		if result.State == runtime.AttemptTimedOut && failureCode == "" {
			failureCode = FailureNodeTimeout
		}
		failure := engine.failure(failureCode, "node_execution", result.Message, dsl.NodeID(node.ExecutionNodeID()), attemptID, result.DSLField, false)
		failure.Details = cloneDetails(result.ErrorDetails)
		target := runtime.NodeFailed
		if result.State == runtime.AttemptTimedOut {
			target = runtime.NodeTimedOut
		}
		if err := node.FailAttempt(attemptID, target, failure); err != nil {
			return err
		}
		_, _ = engine.run.RequestTermination(runtime.TerminationIntent{Kind: runtime.TerminationFailed, RequestedAt: at, Cause: failure})
	case runtime.AttemptCanceled:
		failure := engine.failure(result.ErrorCode, "node_execution", result.Message, dsl.NodeID(node.ExecutionNodeID()), attemptID, result.DSLField, false)
		failure.Details = cloneDetails(result.ErrorDetails)
		if err := node.Cancel(FailureNodeFailed); err != nil {
			return err
		}
		_, _ = engine.run.RequestTermination(runtime.TerminationIntent{Kind: runtime.TerminationFailed, RequestedAt: at, Cause: failure})
	default:
		return fmt.Errorf("attempt result state is unsupported")
	}
	return engine.drive(at)
}

func (engine *Engine) RetryDue(nodeID dsl.NodeID, at time.Time) error {
	if _, terminating := engine.run.Termination(); terminating {
		return engine.drive(at)
	}
	node, exists := engine.nodes[nodeID]
	if !exists {
		return fmt.Errorf("node does not exist")
	}
	if node.State() != runtime.NodeRetryWait {
		return nil
	}
	_, err := node.RetryDue(at)
	return err
}

func (engine *Engine) RequestCancel(at time.Time, cause string) (bool, error) {
	if engine.run.State().Terminal() {
		return false, nil
	}
	applied, err := engine.run.RequestTermination(runtime.TerminationIntent{Kind: runtime.TerminationCanceled, RequestedAt: at, Cause: engine.failure(FailureRunCanceled, "run_control", cause, "", "", "", false)})
	if err != nil {
		return false, err
	}
	return applied, engine.drive(at)
}

func (engine *Engine) DeadlineReached(at time.Time) (bool, error) {
	if engine.run.State().Terminal() {
		return false, nil
	}
	if at.Before(engine.run.DeadlineAt()) {
		return false, nil
	}
	applied, err := engine.run.RequestTermination(runtime.TerminationIntent{Kind: runtime.TerminationTimedOut, RequestedAt: at, Cause: engine.failure(FailureRunTimedOut, "run_deadline", "workflow deadline reached", "", "", "", false)})
	if err != nil {
		return false, err
	}
	return applied, engine.drive(at)
}

func (engine *Engine) CancelAttempt(attemptID string, at time.Time) error {
	accepted, err := engine.RecordAttemptResult(attemptID, runtime.AttemptResult{State: runtime.AttemptCanceled, ErrorCode: "ATTEMPT_CANCELED", Message: "attempt cancellation acknowledged"})
	if err != nil {
		return err
	}
	_ = accepted
	return engine.HandleAttemptCompleted(attemptID, at)
}

func (engine *Engine) initialize() error {
	ids := engine.sortedNodeIDs()
	executionNodeIDs := make([]string, len(ids))
	for index, id := range ids {
		executionNodeIDs[index] = string(id)
	}
	if err := engine.run.Start(executionNodeIDs); err != nil {
		return err
	}
	start := engine.nodes[engine.doc.EntryNodeID]
	if err := start.Activate(); err != nil {
		return err
	}
	outputs, err := engine.startOutputs()
	if err != nil {
		return &Error{Code: FailureInputContract, NodeID: string(engine.doc.EntryNodeID), Field: "workflow_input", Cause: err}
	}
	if err := start.SucceedControl("", outputs); err != nil {
		return err
	}
	engine.settleOutgoing(engine.doc.EntryNodeID, "")
	return engine.drive(time.Time{})
}

func (engine *Engine) drive(at time.Time) error {
	for {
		changed := false
		if intent, terminating := engine.run.Termination(); terminating {
			for _, id := range engine.sortedNodeIDs() {
				node := engine.nodes[id]
				if node.State().Terminal() {
					continue
				}
				switch node.State() {
				case runtime.NodeQueued, runtime.NodeRunning:
					attempt, exists := engine.attempts[node.CurrentAttemptID()]
					if exists && !attempt.State().Terminal() {
						continue
					}
				}
				if err := node.Cancel(cancelReason(intent.Kind)); err != nil {
					return err
				}
				engine.settleOutgoing(id, "")
				changed = true
			}
			if engine.allNodesTerminal() {
				if err := engine.run.CompleteTermination(engine.allNodeRuns()); err != nil {
					return err
				}
			}
			return nil
		}
		for _, id := range engine.sortedNodeIDs() {
			node := engine.nodes[id]
			if node.State() != runtime.NodePending || id == engine.doc.EntryNodeID || !engine.allIncomingSettled(id) {
				continue
			}
			if !engine.anyIncomingActive(id) {
				if err := node.Skip(); err != nil {
					return err
				}
				engine.settleOutgoing(id, "")
				changed = true
				continue
			}
			if err := node.Activate(); err != nil {
				return err
			}
			inputs, ready, err := engine.resolveInputs(engine.nodeDefs[id])
			if err != nil {
				failure := engine.failure(FailureInputContract, "input_binding", err.Error(), id, "", "inputs", false)
				if node.Kind() == runtime.NodeControl {
					if failErr := node.FailControl(failure); failErr != nil {
						return failErr
					}
				} else if failErr := node.FailBeforeAttempt(failure); failErr != nil {
					return failErr
				}
				_, _ = engine.run.RequestTermination(runtime.TerminationIntent{Kind: runtime.TerminationFailed, RequestedAt: atOrCreated(at, engine.run), Cause: failure})
				return engine.drive(at)
			}
			if !ready {
				continue
			}
			definition := engine.nodeDefs[id]
			if definition.Kind == dsl.KindTask {
				if err := node.Ready(inputs); err != nil {
					return err
				}
				changed = true
				continue
			}
			switch definition.Operation.Type {
			case "control.branch":
				var route string
				var branchErr *Error
				if definition.Operation.Version == 2 {
					route, branchErr = evaluateBranchV2(definition, inputs)
				} else {
					route, branchErr = evaluateBranch(definition, inputs["value"])
				}
				if branchErr != nil {
					failure := engine.failure(branchErr.Code, "control_evaluation", branchErr.Error(), id, "", branchErr.Field, false)
					if err := node.FailControl(failure); err != nil {
						return err
					}
					_, _ = engine.run.RequestTermination(runtime.TerminationIntent{Kind: runtime.TerminationFailed, RequestedAt: atOrCreated(at, engine.run), Cause: failure})
					return engine.drive(at)
				}
				if err := node.SucceedControl(route, nil); err != nil {
					return err
				}
				engine.settleOutgoing(id, route)
			case "control.end":
				if !engine.allOtherNodesTerminal(id) {
					continue
				}
				output := inputs["workflow_output"]
				if definition.Operation.Version == 2 {
					values := make(map[string]json.RawMessage, len(inputs))
					for name, value := range inputs {
						values[string(name)] = value
					}
					if err := validateOutputs(definition, values); err != nil {
						return err
					}
					var err error
					output, err = json.Marshal(values)
					if err != nil {
						return err
					}
				}
				if err := runtime.CompleteWorkflowSuccess(engine.run, node, engine.allNodeRuns(), output); err != nil {
					return err
				}
			case "control.start":
				return fmt.Errorf("unexpected second start node")
			default:
				return fmt.Errorf("unsupported control operation %s", definition.Operation.Type)
			}
			changed = true
		}
		if engine.run.State().Terminal() || !changed {
			return nil
		}
	}
}

func (engine *Engine) startOutputs() (map[string]json.RawMessage, error) {
	definition := engine.nodeDefs[engine.doc.EntryNodeID]
	if definition.Operation.Version == 1 {
		return map[string]json.RawMessage{"workflow_input": engine.run.WorkflowInput()}, nil
	}
	var invocation map[string]json.RawMessage
	if err := json.Unmarshal(engine.run.WorkflowInput(), &invocation); err != nil {
		return nil, fmt.Errorf("invalid workflow input: %w", err)
	}
	if len(invocation) != len(definition.Outputs) {
		return nil, fmt.Errorf("workflow input parameter count does not match Start contract")
	}
	for name := range invocation {
		if _, exists := definition.Outputs[dsl.PortName(name)]; !exists {
			return nil, fmt.Errorf("workflow input parameter %s is undeclared", name)
		}
	}
	if err := validateOutputs(definition, invocation); err != nil {
		return nil, err
	}
	return invocation, nil
}

func (engine *Engine) acceptSuccess(node *runtime.NodeRun, definition dsl.Node, attemptID string, outputs map[string]json.RawMessage) error {
	if err := validateOutputs(definition, outputs); err != nil {
		return err
	}
	if err := node.SucceedAttempt(attemptID, outputs); err != nil {
		return err
	}
	engine.settleOutgoing(definition.ID, "")
	return nil
}

func (engine *Engine) resolveInputs(definition dsl.Node) (map[string]json.RawMessage, bool, error) {
	result := make(map[string]json.RawMessage, len(definition.Inputs))
	for name, binding := range definition.Inputs {
		switch binding.Kind {
		case dsl.BindingLiteral:
			result[string(name)] = cloneRaw(binding.Value)
		case dsl.BindingNodeOutput:
			if binding.Output == nil {
				return nil, false, fmt.Errorf("binding output is missing")
			}
			source := engine.nodes[binding.Output.NodeID]
			if source == nil {
				return nil, false, fmt.Errorf("source node is missing")
			}
			if source.State() != runtime.NodeSucceeded {
				if source.State().Terminal() {
					return nil, false, fmt.Errorf("source node did not succeed")
				}
				return nil, false, nil
			}
			value, exists := source.EffectiveOutputs()[string(binding.Output.Name)]
			if !exists {
				return nil, false, fmt.Errorf("effective output is missing")
			}
			result[string(name)] = value
		default:
			return nil, false, fmt.Errorf("input binding kind is unsupported")
		}
	}
	return result, true, nil
}

func (engine *Engine) settleOutgoing(source dsl.NodeID, selectedRoute string) {
	for _, edge := range engine.outgoing[source] {
		state := EdgeInactive
		sourceState := engine.nodes[source].State()
		if sourceState == runtime.NodeSucceeded {
			if edge.Activation.Kind == dsl.ActivationAlways || edge.Activation.Kind == dsl.ActivationRoute && string(edge.Activation.Route) == selectedRoute {
				state = EdgeActive
			}
		}
		engine.edges[edge.ID] = state
	}
}

func (engine *Engine) allIncomingSettled(id dsl.NodeID) bool {
	for _, edge := range engine.incoming[id] {
		if engine.edges[edge.ID] == EdgePending {
			return false
		}
	}
	return true
}

func (engine *Engine) anyIncomingActive(id dsl.NodeID) bool {
	for _, edge := range engine.incoming[id] {
		if engine.edges[edge.ID] == EdgeActive {
			return true
		}
	}
	return false
}

func (engine *Engine) allOtherNodesTerminal(except dsl.NodeID) bool {
	for id, node := range engine.nodes {
		if id != except && !node.State().Terminal() {
			return false
		}
	}
	return true
}

func (engine *Engine) allNodesTerminal() bool {
	for _, node := range engine.nodes {
		if !node.State().Terminal() {
			return false
		}
	}
	return true
}

func (engine *Engine) allNodeRuns() []*runtime.NodeRun {
	result := make([]*runtime.NodeRun, 0, len(engine.nodes))
	for _, id := range engine.sortedNodeIDs() {
		result = append(result, engine.nodes[id])
	}
	return result
}

func (engine *Engine) sortedNodeIDs() []dsl.NodeID {
	ids := make([]dsl.NodeID, 0, len(engine.nodes))
	for id := range engine.nodes {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

func (engine *Engine) failure(code, phase, message string, nodeID dsl.NodeID, attemptID, field string, retryable bool) runtime.Failure {
	return runtime.Failure{
		Code: code, Phase: phase, Retryable: retryable,
		RunID: engine.run.ID(), SnapshotID: engine.snapshot.ID, DefinitionHash: engine.snapshot.DefinitionHash,
		ExecutionNodeID: string(nodeID), DSLField: field, AttemptID: attemptID, Message: message,
	}
}

func retryable(policy dsl.ExecutionPolicy, code string) bool {
	for _, candidate := range policy.RetryableErrorCodes {
		if candidate == code {
			return true
		}
	}
	return false
}

func retryDueAt(at time.Time, policy dsl.ExecutionPolicy) time.Time {
	return at.Add(time.Duration(policy.RetryBackoff.DelayMS) * time.Millisecond)
}

func cancelReason(kind runtime.TerminationKind) string {
	switch kind {
	case runtime.TerminationCanceled:
		return FailureRunCanceled
	case runtime.TerminationTimedOut:
		return FailureRunTimedOut
	default:
		return FailureNodeFailed
	}
}

func cloneDetails(value map[string]any) map[string]any {
	if value == nil {
		return nil
	}
	result := make(map[string]any, len(value))
	for key, item := range value {
		result[key] = item
	}
	return result
}

func atOrCreated(at time.Time, run *runtime.WorkflowRun) time.Time {
	if !at.IsZero() {
		return at
	}
	return run.CreatedAt()
}

func validateOutputs(definition dsl.Node, values map[string]json.RawMessage) error {
	if len(values) != len(definition.Outputs) {
		return fmt.Errorf("output field count does not match DSL contract")
	}
	for name, dataType := range definition.Outputs {
		value, exists := values[string(name)]
		if !exists {
			return fmt.Errorf("required output %s is missing", name)
		}
		actual, ok := rawDataType(value)
		if !ok || actual != dataType && !(actual == dsl.TypeInteger && dataType == dsl.TypeNumber) {
			return fmt.Errorf("output %s has type %s, expected %s", name, actual, dataType)
		}
	}
	return nil
}

func rawDataType(raw json.RawMessage) (dsl.DataType, bool) {
	if !json.Valid(raw) || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return "", false
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if decoder.Decode(&value) != nil {
		return "", false
	}
	switch value.(type) {
	case string:
		return dsl.TypeString, true
	case bool:
		return dsl.TypeBoolean, true
	case json.Number:
		rational, ok := new(big.Rat).SetString(value.(json.Number).String())
		if !ok {
			return "", false
		}
		if rational.IsInt() {
			return dsl.TypeInteger, true
		}
		return dsl.TypeNumber, true
	case []any:
		return dsl.TypeArray, true
	case map[string]any:
		return dsl.TypeObject, true
	default:
		return "", false
	}
}

func cloneRaw(raw json.RawMessage) json.RawMessage { return append(json.RawMessage(nil), raw...) }

func cloneDocument(document dsl.Document) (dsl.Document, error) {
	payload, err := json.Marshal(document)
	if err != nil {
		return dsl.Document{}, err
	}
	var result dsl.Document
	if err := json.Unmarshal(payload, &result); err != nil {
		return dsl.Document{}, err
	}
	return result, nil
}
