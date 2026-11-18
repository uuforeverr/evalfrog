package definition

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/uu999/evalfrog/internal/access"
	"github.com/uu999/evalfrog/internal/compiler"
	"github.com/uu999/evalfrog/internal/ir"
	"github.com/uu999/evalfrog/internal/resources"
)

type revisionCapture struct {
	Repository
	base     DraftRevision
	saved    SaveDraftRecord
	conflict bool
}

func (capture *revisionCapture) GetDraftRevision(_ context.Context, _, _ string, _ int64) (DraftRevision, error) {
	return capture.base, nil
}
func (capture *revisionCapture) SaveDraft(_ context.Context, record SaveDraftRecord) (DraftRevision, error) {
	capture.saved = record
	if capture.conflict {
		return DraftRevision{}, ErrDraftRevisionConflict
	}
	return DraftRevision{CatalogRevision: record.CatalogRevision, RevisionNumber: record.ExpectedRevision + 1, IRJSON: record.IRJSON}, nil
}
func (capture *revisionCapture) GetCurrentDraft(context.Context, string, string) (Draft, error) {
	return Draft{CurrentRevision: 2, Current: DraftRevision{IRJSON: capture.base.IRJSON}}, nil
}

type allowRevisionTest struct{}

func (allowRevisionTest) Require(context.Context, string, string, access.Permission) error {
	return nil
}

type unusedResources struct{}

func (unusedResources) Resolve(context.Context, string, string, resources.Purpose, ir.Document) (compiler.ResourceBindings, []ir.Diagnostic, error) {
	return compiler.EmptyResourceBindings(), nil, nil
}

func TestSaveDraftCanUpgradeCatalogRevisionWithoutChangingBase(t *testing.T) {
	store := &revisionCapture{base: DraftRevision{CatalogRevision: "catalog-v1", RevisionNumber: 1}}
	service := NewBuiltinService(store, allowRevisionTest{}, unusedResources{})
	for _, test := range []struct{ version, expected string }{{"1", "catalog-v1"}, {"2", "catalog-v2"}} {
		raw, err := json.Marshal(map[string]any{"ir_version": test.version, "nodes": []any{}, "edges": []any{}, "layout": map[string]any{}})
		if err != nil {
			t.Fatal(err)
		}
		result, diagnostics, err := service.SaveDraft(context.Background(), SaveDraftCommand{ProjectID: "project", PrincipalID: "principal", WorkflowID: "workflow", ExpectedRevision: 1, IRJSON: raw, IdempotencyKey: "unique-key-123"})
		if err != nil || ir.HasErrors(diagnostics) {
			t.Fatalf("save version %s: result=%+v diagnostics=%+v err=%v", test.version, result, diagnostics, err)
		}
		if store.saved.CatalogRevision != test.expected || result.CatalogRevision != test.expected {
			t.Fatalf("version %s used catalog %s", test.version, store.saved.CatalogRevision)
		}
	}
	if store.base.CatalogRevision != "catalog-v1" {
		t.Fatal("base revision catalog changed")
	}
}

func TestSaveConflictSubmittedIRIsJSONObject(t *testing.T) {
	raw := json.RawMessage(`{"ir_version":"2","nodes":[],"edges":[],"layout":{}}`)
	store := &revisionCapture{base: DraftRevision{RevisionNumber: 1, IRJSON: raw}, conflict: true}
	service := NewBuiltinService(store, allowRevisionTest{}, unusedResources{})
	_, _, err := service.SaveDraft(context.Background(), SaveDraftCommand{ProjectID: "project", PrincipalID: "principal", WorkflowID: "workflow", ExpectedRevision: 1, IRJSON: raw, IdempotencyKey: "conflict-key-123"})
	var conflict *Error
	if !errors.As(err, &conflict) || conflict.Code != CodeDraftRevisionConflict {
		t.Fatalf("expected conflict, got %v", err)
	}
	encoded, err := json.Marshal(conflict.Details)
	if err != nil {
		t.Fatal(err)
	}
	var details map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &details); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"base_ir", "current_ir", "submitted_ir"} {
		if len(details[key]) == 0 || details[key][0] != '{' {
			t.Fatalf("%s must be JSON object: %s", key, details[key])
		}
	}
}
