package engine

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/uu999/evalfrog/internal/dsl"
	"github.com/uu999/evalfrog/internal/runtime"
)

func TestNamedStartEndRunAndInvocationContract(t *testing.T) {
	startID := dsl.NodeID("xn_000000000000000000000001")
	endID := dsl.NodeID("xn_000000000000000000000002")
	document := dsl.Document{DSLVersion: dsl.VersionV1, EntryNodeID: startID, ExitNodeID: endID, Nodes: []dsl.Node{
		{ID: startID, Kind: dsl.KindControl, Operation: dsl.Operation{Type: "control.start", Version: 2, Config: map[string]json.RawMessage{}}, Inputs: map[dsl.PortName]dsl.InputBinding{}, Outputs: map[dsl.PortName]dsl.DataType{"amount": dsl.TypeInteger}},
		{ID: endID, Kind: dsl.KindControl, Operation: dsl.Operation{Type: "control.end", Version: 2, Config: map[string]json.RawMessage{}}, Inputs: map[dsl.PortName]dsl.InputBinding{"total": {Kind: dsl.BindingNodeOutput, DataType: dsl.TypeInteger, Output: &dsl.OutputReference{NodeID: startID, Name: "amount"}}}, Outputs: map[dsl.PortName]dsl.DataType{"total": dsl.TypeInteger}},
	}, Edges: []dsl.Edge{{ID: "xe_000000000000000000000001", SourceNodeID: startID, TargetNodeID: endID, Activation: dsl.Activation{Kind: dsl.ActivationAlways}}}}
	snapshot := Snapshot{ID: "snapshot", DefinitionHash: "hash", DSL: document}
	now := time.Unix(1_700_000_000, 0).UTC()
	command := runtime.CreateRunCommand{RunID: "run", ProjectID: "project", WorkflowID: "workflow", Purpose: runtime.RunPurposeTest, Definition: runtime.DefinitionReference{SnapshotID: snapshot.ID, DefinitionHash: snapshot.DefinitionHash, Source: runtime.DefinitionDraftSnapshot}, WorkflowInput: json.RawMessage(`{"amount":7}`), CreatedAt: now, DeadlineAt: now.Add(time.Hour)}
	engine, err := NewBuiltinV1(snapshot, command)
	if err != nil {
		t.Fatal(err)
	}
	if engine.Run().State() != runtime.RunSucceeded || string(engine.Run().WorkflowOutput()) != `{"total":7}` {
		t.Fatalf("state=%s output=%s", engine.Run().State(), engine.Run().WorkflowOutput())
	}
	command.WorkflowInput = json.RawMessage(`{"amount":"wrong"}`)
	if _, err := NewBuiltinV1(snapshot, command); err == nil {
		t.Fatal("wrong invocation type was accepted")
	} else if failure, ok := err.(*Error); !ok || failure.Code != FailureInputContract {
		t.Fatalf("wrong invocation error: %v", err)
	}
}

func TestBranchV2OrderedAndOrConditions(t *testing.T) {
	node := dsl.Node{ID: "xn_000000000000000000000003", Operation: dsl.Operation{Type: "control.branch", Version: 2, Config: map[string]json.RawMessage{
		"cases":         json.RawMessage(`[{"route":"pass","logic":"and","conditions":[{"input":"score","operator":"gte","value":60},{"input":"enabled","operator":"eq","value":true}]},{"route":"review","logic":"or","conditions":[{"input":"score","operator":"gte","value":60},{"input":"enabled","operator":"eq","value":true}]}]`),
		"default_route": json.RawMessage(`"else"`),
	}}}
	route, failure := evaluateBranchV2(node, map[string]json.RawMessage{"score": json.RawMessage(`70`), "enabled": json.RawMessage(`false`)})
	if failure != nil || route != "review" {
		t.Fatalf("route=%s failure=%v", route, failure)
	}
	route, failure = evaluateBranchV2(node, map[string]json.RawMessage{"score": json.RawMessage(`70`), "enabled": json.RawMessage(`true`)})
	if failure != nil || route != "pass" {
		t.Fatalf("route=%s failure=%v", route, failure)
	}
}
