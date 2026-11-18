package engine

import (
	"encoding/json"
	"fmt"
	"sort"

	"github.com/uu999/evalfrog/internal/dsl"
	"github.com/uu999/evalfrog/internal/runtime"
)

// State is the complete, persistence-safe Engine aggregate. Edges are not
// persisted because their state is a deterministic projection of immutable
// DSL plus the source Node Run terminal facts.
type State struct {
	Run      runtime.WorkflowRunRecord
	Nodes    []runtime.NodeRunRecord
	Attempts []runtime.NodeAttemptRecord
}

func (engine *Engine) SnapshotState() State {
	state := State{Run: engine.run.Snapshot()}
	for _, id := range engine.sortedNodeIDs() {
		state.Nodes = append(state.Nodes, engine.nodes[id].Snapshot())
	}
	attemptIDs := make([]string, 0, len(engine.attempts))
	for id := range engine.attempts {
		attemptIDs = append(attemptIDs, id)
	}
	sort.Strings(attemptIDs)
	for _, id := range attemptIDs {
		state.Attempts = append(state.Attempts, engine.attempts[id].Snapshot())
	}
	return state
}

func RestoreBuiltinV1(snapshot Snapshot, state State) (*Engine, error) {
	return Restore(snapshot, state, dsl.BuiltinV2Contract(), dsl.BuiltinV2Compatibility())
}

func Restore(snapshot Snapshot, state State, contract dsl.Contract, compatibility dsl.CompatibilityChecker) (*Engine, error) {
	command := runtime.CreateRunCommand{
		RunID: state.Run.ID, ProjectID: state.Run.ProjectID, WorkflowID: state.Run.WorkflowID,
		Purpose: state.Run.Purpose, Definition: state.Run.Definition,
		WorkflowInput: state.Run.WorkflowInput, DeadlineAt: state.Run.DeadlineAt, CreatedAt: state.Run.CreatedAt,
	}
	instance, err := New(snapshot, command, contract, compatibility)
	if err != nil {
		return nil, err
	}
	run, err := runtime.RestoreWorkflowRun(state.Run)
	if err != nil {
		return nil, err
	}
	if run.Definition().SnapshotID != snapshot.ID || run.Definition().DefinitionHash != snapshot.DefinitionHash {
		return nil, &Error{Code: "RUNTIME_SNAPSHOT_BINDING_INVALID"}
	}
	if len(state.Nodes) != len(instance.nodeDefs) {
		return nil, fmt.Errorf("persisted node set is incomplete")
	}
	nodes := make(map[dsl.NodeID]*runtime.NodeRun, len(state.Nodes))
	for _, record := range state.Nodes {
		id := dsl.NodeID(record.ExecutionNodeID)
		definition, exists := instance.nodeDefs[id]
		if !exists {
			return nil, fmt.Errorf("persisted node %q is absent from immutable DSL", id)
		}
		expectedKind := runtime.NodeTask
		if definition.Kind == dsl.KindControl {
			expectedKind = runtime.NodeControl
		}
		if record.RunID != run.ID() || record.Kind != expectedKind {
			return nil, fmt.Errorf("persisted node %q identity or kind disagrees with immutable DSL", id)
		}
		if id == instance.doc.EntryNodeID && record.State == runtime.NodeSucceeded && len(record.EffectiveOutputs) == 0 {
			record.EffectiveOutputs = map[string]json.RawMessage{"workflow_input": run.WorkflowInput()}
		}
		node, restoreErr := runtime.RestoreNodeRun(record)
		if restoreErr != nil {
			return nil, restoreErr
		}
		if _, duplicate := nodes[id]; duplicate {
			return nil, fmt.Errorf("persisted node %q is duplicated", id)
		}
		nodes[id] = node
	}
	attempts := make(map[string]*runtime.NodeAttempt, len(state.Attempts))
	attemptNodes := make(map[string]dsl.NodeID, len(state.Attempts))
	for _, record := range state.Attempts {
		attempt, restoreErr := runtime.RestoreNodeAttempt(record)
		if restoreErr != nil {
			return nil, restoreErr
		}
		var owner dsl.NodeID
		for id, node := range nodes {
			if record.NodeRunID == node.RunID()+":"+node.ExecutionNodeID() {
				owner = id
				break
			}
		}
		if owner == "" {
			return nil, fmt.Errorf("attempt %q references an unknown node run", record.ID)
		}
		if _, duplicate := attempts[record.ID]; duplicate {
			return nil, fmt.Errorf("persisted attempt %q is duplicated", record.ID)
		}
		attempts[record.ID] = attempt
		attemptNodes[record.ID] = owner
	}
	for id, node := range nodes {
		if current := node.CurrentAttemptID(); current != "" {
			_, exists := attempts[current]
			if !exists || attemptNodes[current] != id {
				return nil, fmt.Errorf("node %q current attempt is missing or belongs to another node", id)
			}
		}
	}
	instance.run = run
	instance.nodes = nodes
	instance.attempts = attempts
	instance.attemptNodes = attemptNodes
	for id := range instance.edges {
		instance.edges[id] = EdgePending
	}
	for _, id := range instance.sortedNodeIDs() {
		node := instance.nodes[id]
		if node.State().Terminal() {
			instance.settleOutgoing(id, node.SelectedRoute())
		}
	}
	return instance, nil
}
