package compiler_test

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/uu999/evalfrog/internal/catalog"
	"github.com/uu999/evalfrog/internal/compiler"
	"github.com/uu999/evalfrog/internal/ir"
	"github.com/uu999/evalfrog/internal/runtime"
	"github.com/uu999/evalfrog/internal/runtime/engine"
)

func TestV2NamedStartAndEndCompileDeterministically(t *testing.T) {
	document := ir.Document{IRVersion: ir.VersionV2, Nodes: []ir.Node{
		{ID: "start", Type: "start", Title: "开始", Inputs: []ir.Input{}, Outputs: []ir.Output{{Name: "amount", DataType: ir.TypeInteger}}},
		{ID: "end", Type: "end", Title: "结束", Inputs: []ir.Input{{Name: "total", DataType: ir.TypeInteger, Source: ir.SourceRef, RefNode: "start", RefOutput: "amount"}}, Outputs: []ir.Output{{Name: "total", DataType: ir.TypeInteger}}},
	}, Edges: []ir.Edge{{ID: "flow", Source: "start", Target: "end"}}, Layout: v2Layout("start", "end")}
	request := compiler.Request{IR: document, Catalog: catalog.BuiltinV2(), Policy: compiler.DefaultPolicyV1(), Resources: compiler.EmptyResourceBindings()}
	first, diagnostics := compiler.BuiltinV2().Compile(request)
	if ir.HasErrors(diagnostics) {
		t.Fatalf("v2 compile: %+v", diagnostics)
	}
	second, diagnostics := compiler.BuiltinV2().Compile(request)
	if ir.HasErrors(diagnostics) || string(first.CanonicalDSL) != string(second.CanonicalDSL) {
		t.Fatalf("v2 compile is not deterministic: %+v", diagnostics)
	}
	if len(first.DSL.Nodes) != 2 {
		t.Fatalf("unexpected DSL nodes: %+v", first.DSL.Nodes)
	}
	for _, node := range first.DSL.Nodes {
		switch node.Operation.Type {
		case "control.start":
			if node.Operation.Version != 2 || node.Outputs["amount"] != "integer" {
				t.Fatalf("start contract: %+v", node)
			}
		case "control.end":
			if node.Operation.Version != 2 || node.Inputs["total"].Output == nil || node.Outputs["total"] != "integer" {
				t.Fatalf("end contract: %+v", node)
			}
		}
	}
	document.Nodes[1].Outputs[0].Name = "other"
	_, diagnostics = compiler.BuiltinV2().Compile(compiler.Request{IR: document, Catalog: catalog.BuiltinV2(), Policy: compiler.DefaultPolicyV1(), Resources: compiler.EmptyResourceBindings()})
	if !ir.HasErrors(diagnostics) {
		t.Fatal("mismatched End output binding was accepted")
	}
}

func TestV2BranchConditionGroupCompiles(t *testing.T) {
	document := ir.Document{IRVersion: ir.VersionV2, Nodes: []ir.Node{
		{ID: "start", Type: "start", Title: "开始", Inputs: []ir.Input{}, Outputs: []ir.Output{{Name: "score", DataType: ir.TypeInteger}}},
		{ID: "branch", Type: "branch", Title: "判断", Inputs: []ir.Input{
			{Name: "score", DataType: ir.TypeInteger, Source: ir.SourceRef, RefNode: "start", RefOutput: "score"},
			{Name: "cases", DataType: ir.TypeArray, Source: ir.SourceLiteral, Value: json.RawMessage(`[{"route":"pass","logic":"and","conditions":[{"input":"score","operator":"gte","value":60}]}]`)},
			{Name: "default_route", DataType: ir.TypeString, Source: ir.SourceLiteral, Value: json.RawMessage(`"else"`)},
		}, Outputs: []ir.Output{}},
		{ID: "end", Type: "end", Title: "结束", Inputs: []ir.Input{{Name: "result", DataType: ir.TypeString, Source: ir.SourceLiteral, Value: json.RawMessage(`"done"`)}}, Outputs: []ir.Output{{Name: "result", DataType: ir.TypeString}}},
	}, Edges: []ir.Edge{{ID: "first", Source: "start", Target: "branch"}, {ID: "pass", Source: "branch", Target: "end", Route: "pass"}, {ID: "else", Source: "branch", Target: "end", Route: "else"}}, Layout: v2Layout("start", "branch", "end")}
	result, diagnostics := compiler.BuiltinV2().Compile(compiler.Request{IR: document, Catalog: catalog.BuiltinV2(), Policy: compiler.DefaultPolicyV1(), Resources: compiler.EmptyResourceBindings()})
	if ir.HasErrors(diagnostics) {
		t.Fatalf("branch v2 compile: %+v", diagnostics)
	}
	for _, node := range result.DSL.Nodes {
		if node.Operation.Type == "control.branch" && node.Operation.Version != 2 {
			t.Fatalf("branch operation: %+v", node.Operation)
		}
		if node.Operation.Type == "control.branch" && result.SourceMap.Fields[node.ID]["operation.config.cases.0.conditions.0.value"] != "inputs.cases.0.conditions.0.value" {
			t.Fatal("branch condition value has no precise IR field mapping")
		}
	}
	snapshot := engine.Snapshot{ID: "snapshot", DefinitionHash: result.Hashes.DefinitionHash, DSL: result.DSL}
	now := time.Unix(1_700_000_000, 0).UTC()
	command := runtime.CreateRunCommand{RunID: "run", ProjectID: "project", WorkflowID: "workflow", Purpose: runtime.RunPurposeTest, Definition: runtime.DefinitionReference{SnapshotID: snapshot.ID, DefinitionHash: snapshot.DefinitionHash, Source: runtime.DefinitionDraftSnapshot}, WorkflowInput: json.RawMessage(`{"score":70}`), CreatedAt: now, DeadlineAt: now.Add(time.Hour)}
	instance, err := engine.NewBuiltinV1(snapshot, command)
	if err != nil {
		t.Fatal(err)
	}
	if instance.Run().State() != runtime.RunSucceeded || string(instance.Run().WorkflowOutput()) != `{"result":"done"}` {
		t.Fatalf("run=%s output=%s", instance.Run().State(), instance.Run().WorkflowOutput())
	}
}

func v2Layout(ids ...ir.LogicalID) map[ir.LogicalID]ir.Position {
	result := make(map[ir.LogicalID]ir.Position, len(ids))
	for index, id := range ids {
		result[id] = ir.Position{X: numberPointer(index * 200), Y: numberPointer(100)}
	}
	return result
}

func numberPointer(value int) *json.Number { number := json.Number(fmt.Sprint(value)); return &number }
