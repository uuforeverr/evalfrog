package context

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/uu999/evalfrog/internal/dsl"
	"github.com/uu999/evalfrog/internal/scheduling"
)

func TestGatewayKeepsMySQLAuthoritativeAndCachesOnlyParsedSnapshot(t *testing.T) {
	repository := &fakeRepository{metadata: metadataFixture(), snapshot: snapshotFixture(), runInput: json.RawMessage(`{"run":true}`), outputs: map[string]json.RawMessage{"upstream": json.RawMessage(`{"value":7}`)}}
	cache, _ := NewSnapshotCache(8, 1<<20)
	gateway, err := NewGateway(repository, cache)
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 2; index++ {
		value, loadErr := gateway.Load(context.Background(), loadFixture())
		if loadErr != nil {
			t.Fatal(loadErr)
		}
		if string(value.UpstreamOutputs["start"]) != `{"run":true}` || string(value.UpstreamOutputs["upstream"]) != `{"value":7}` {
			t.Fatalf("context=%+v", value)
		}
	}
	if repository.snapshotReads != 1 || repository.runContextReads != 2 {
		t.Fatalf("snapshot reads=%d run-context reads=%d", repository.snapshotReads, repository.runContextReads)
	}
}

func TestGatewayDoesNotExposeMutableSnapshotMaps(t *testing.T) {
	repository := &fakeRepository{metadata: metadataFixture(), snapshot: snapshotFixture(), runInput: json.RawMessage(`{}`), outputs: map[string]json.RawMessage{"upstream": json.RawMessage(`7`)}}
	cache, _ := NewSnapshotCache(8, 1<<20)
	gateway, _ := NewGateway(repository, cache)
	first, err := gateway.Load(context.Background(), loadFixture())
	if err != nil {
		t.Fatal(err)
	}
	first.Operation.Config["injected"] = json.RawMessage(`true`)
	first.OutputContract["result"] = dsl.TypeString
	second, err := gateway.Load(context.Background(), loadFixture())
	if err != nil {
		t.Fatal(err)
	}
	if _, exists := second.Operation.Config["injected"]; exists || second.OutputContract["result"] != dsl.TypeInteger {
		t.Fatalf("cached snapshot was mutated: operation=%+v output=%+v", second.Operation, second.OutputContract)
	}
}

func TestGatewayLoadBatchReadsOneRunContextPerRun(t *testing.T) {
	repository := &fakeRepository{metadata: metadataFixture(), snapshot: snapshotFixture(), runInput: json.RawMessage(`{}`), outputs: map[string]json.RawMessage{"upstream": json.RawMessage(`7`)}}
	cache, _ := NewSnapshotCache(8, 1<<20)
	gateway, _ := NewGateway(repository, cache)
	commands := []LoadCommand{loadFixture(), loadFixture()}
	commands[1].AttemptID = "attempt-2"
	results := gateway.LoadBatch(context.Background(), commands)
	if len(results) != 2 || results[0].Err != nil || results[1].Err != nil {
		t.Fatalf("results=%+v", results)
	}
	if repository.metadataReads != 1 || repository.snapshotReads != 1 || repository.runContextReads != 1 {
		t.Fatalf("metadata=%d snapshot=%d run-context=%d", repository.metadataReads, repository.snapshotReads, repository.runContextReads)
	}
	if len(repository.requestedOutputs) != 1 || repository.requestedOutputs[0] != "upstream" {
		t.Fatalf("requested outputs=%v", repository.requestedOutputs)
	}
}

func TestGatewayBatchKeepsInvalidAttemptIsolated(t *testing.T) {
	repository := &fakeRepository{metadata: metadataFixture(), snapshot: snapshotFixture(), runInput: json.RawMessage(`{}`), outputs: map[string]json.RawMessage{"upstream": json.RawMessage(`7`)}}
	repository.metadataResults = []MetadataResult{{Err: errors.New("lease expired")}, {Metadata: repository.metadata}}
	cache, _ := NewSnapshotCache(8, 1<<20)
	gateway, _ := NewGateway(repository, cache)
	commands := []LoadCommand{loadFixture(), loadFixture(), {}}
	commands[1].AttemptID = "valid-attempt"
	results := gateway.LoadBatch(context.Background(), commands)
	if len(results) != 3 || results[0].Err == nil || results[1].Err != nil || results[2].Err == nil {
		t.Fatalf("batch results=%+v", results)
	}
	if repository.metadataReads != 1 || repository.snapshotReads != 1 || repository.runContextReads != 1 {
		t.Fatalf("reads=%d/%d/%d", repository.metadataReads, repository.snapshotReads, repository.runContextReads)
	}
}

func TestGatewayRejectsInvalidOrInconsistentAuthority(t *testing.T) {
	cache, _ := NewSnapshotCache(8, 1<<20)
	if _, err := NewGateway(nil, cache); err == nil {
		t.Fatal("nil repository accepted")
	}
	repository := &fakeRepository{metadata: metadataFixture(), snapshot: snapshotFixture(), runInput: json.RawMessage(`{}`), outputs: map[string]json.RawMessage{"upstream": json.RawMessage(`7`)}}
	gateway, _ := NewGateway(repository, cache)
	if _, err := gateway.Load(context.Background(), LoadCommand{}); err == nil {
		t.Fatal("invalid command accepted")
	}
	repository.metadata.Operation.Type = "task.rpc"
	if _, err := gateway.Load(context.Background(), loadFixture()); err == nil {
		t.Fatal("snapshot coordinate mismatch accepted")
	}
}

func TestGatewayDoesNotCacheCorruptSnapshot(t *testing.T) {
	repository := &fakeRepository{metadata: metadataFixture(), snapshot: json.RawMessage(`{"bad":`), runInput: json.RawMessage(`{}`)}
	cache, _ := NewSnapshotCache(8, 1<<20)
	gateway, _ := NewGateway(repository, cache)
	for index := 0; index < 2; index++ {
		if _, err := gateway.Load(context.Background(), loadFixture()); err == nil {
			t.Fatal("corrupt snapshot accepted")
		}
	}
	if repository.snapshotReads != 2 {
		t.Fatalf("corrupt snapshot was cached: reads=%d", repository.snapshotReads)
	}
}

func TestGatewayReturnsAuthoritativeRunContextErrors(t *testing.T) {
	repository := &fakeRepository{metadata: metadataFixture(), snapshot: snapshotFixture(), runErr: errors.New("mysql unavailable")}
	cache, _ := NewSnapshotCache(8, 1<<20)
	gateway, _ := NewGateway(repository, cache)
	if _, err := gateway.Load(context.Background(), loadFixture()); !errors.Is(err, repository.runErr) {
		t.Fatalf("error=%v", err)
	}
}

func TestExecutionNodeSearchesAndValidatesCoordinate(t *testing.T) {
	metadata := metadataFixture()
	document := dsl.Document{Nodes: []dsl.Node{{ID: "other", Operation: dsl.Operation{Type: "task.python", Version: 1}}, {ID: "code", Operation: dsl.Operation{Type: "task.python", Version: 1}}}}
	raw, _ := json.Marshal(document)
	if node, err := executionNode(raw, metadata); err != nil || node.ID != "code" {
		t.Fatalf("node=%+v err=%v", node, err)
	}
	metadata.ExecutionNodeID = "missing"
	if _, err := executionNode(raw, metadata); err == nil {
		t.Fatal("missing node accepted")
	}
}

type fakeRepository struct {
	metadata                                      Metadata
	snapshot, runInput                            json.RawMessage
	outputs                                       map[string]json.RawMessage
	metadataErr, snapshotErr, runErr              error
	metadataResults                               []MetadataResult
	metadataReads, snapshotReads, runContextReads int
	requestedOutputs                              []string
}

func (value *fakeRepository) LoadAttemptMetadataBatch(_ context.Context, commands []LoadCommand) []MetadataResult {
	value.metadataReads++
	if value.metadataResults != nil {
		return value.metadataResults
	}
	results := make([]MetadataResult, len(commands))
	for index := range results {
		results[index] = MetadataResult{Metadata: value.metadata, Err: value.metadataErr}
	}
	return results
}

func (value *fakeRepository) LoadSnapshotDSL(context.Context, string, string) (json.RawMessage, error) {
	value.snapshotReads++
	return value.snapshot, value.snapshotErr
}

func (value *fakeRepository) LoadRunContext(_ context.Context, _, _ string, ids []string) (json.RawMessage, map[string]json.RawMessage, error) {
	value.runContextReads++
	value.requestedOutputs = append([]string(nil), ids...)
	return value.runInput, value.outputs, value.runErr
}

func loadFixture() LoadCommand {
	return LoadCommand{ProjectID: "project", RunID: "run", AttemptID: "attempt", AttemptSequence: 1, LeaseToken: "lease", FencingToken: 1}
}

func metadataFixture() Metadata {
	return Metadata{ProjectID: "project", RunID: "run", NodeRunID: "node-run", ExecutionNodeID: "code", AttemptID: "attempt", AttemptSequence: 1, SnapshotID: "snapshot", Operation: dsl.Coordinate{Type: "task.python", Version: 1}, ResourceClass: scheduling.ResourceSandbox, ResolvedInputs: map[string]json.RawMessage{"result": json.RawMessage(`7`)}}
}

func snapshotFixture() json.RawMessage {
	document := dsl.Document{DSLVersion: "1", EntryNodeID: "start", ExitNodeID: "end", Nodes: []dsl.Node{
		{ID: "start", Kind: dsl.KindControl, Operation: dsl.Operation{Type: "control.start", Version: 1}, Outputs: map[dsl.PortName]dsl.DataType{"workflow_input": dsl.TypeObject}},
		{ID: "upstream", Kind: dsl.KindTask, Operation: dsl.Operation{Type: "task.python", Version: 1}, Outputs: map[dsl.PortName]dsl.DataType{"result": dsl.TypeInteger}},
		{ID: "code", Kind: dsl.KindTask, Operation: dsl.Operation{Type: "task.python", Version: 1, Config: map[string]json.RawMessage{}}, Inputs: map[dsl.PortName]dsl.InputBinding{
			"source":   {Kind: dsl.BindingNodeOutput, DataType: dsl.TypeObject, Output: &dsl.OutputReference{NodeID: "start", Name: "workflow_input"}},
			"previous": {Kind: dsl.BindingNodeOutput, DataType: dsl.TypeInteger, Output: &dsl.OutputReference{NodeID: "upstream", Name: "result"}},
		}, Outputs: map[dsl.PortName]dsl.DataType{"result": dsl.TypeInteger}, ExecutionPolicy: dsl.ExecutionPolicy{AttemptTimeoutMS: 1000}},
	}}
	raw, _ := json.Marshal(document)
	return raw
}
