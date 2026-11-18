package definition

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/uu999/evalfrog/internal/access"
	"github.com/uu999/evalfrog/internal/catalog"
	compilerpkg "github.com/uu999/evalfrog/internal/compiler"
	"github.com/uu999/evalfrog/internal/ir"
	"github.com/uu999/evalfrog/internal/platform/identity"
	"github.com/uu999/evalfrog/internal/resources"
)

type Service struct {
	repository Repository
	access     AccessControl
	resources  ResourceResolver
	compiler   Compiler
	catalogs   CatalogRegistry
	policy     compilerpkg.Policy
	ids        identity.Generator
	parser     ir.Parser
}

type CatalogRegistry interface {
	Current() compilerpkg.NodeCatalog
	Get(string) (compilerpkg.NodeCatalog, bool)
}

type StaticCatalogRegistry struct {
	current  string
	catalogs map[string]compilerpkg.NodeCatalog
}

func NewStaticCatalogRegistry(current compilerpkg.NodeCatalog, catalogs ...compilerpkg.NodeCatalog) (StaticCatalogRegistry, error) {
	if current == nil || current.Revision() == "" {
		return StaticCatalogRegistry{}, fmt.Errorf("current catalog is required")
	}
	result := StaticCatalogRegistry{current: string(current.Revision()), catalogs: make(map[string]compilerpkg.NodeCatalog, len(catalogs)+1)}
	for _, value := range append([]compilerpkg.NodeCatalog{current}, catalogs...) {
		if value == nil || value.Revision() == "" {
			return StaticCatalogRegistry{}, fmt.Errorf("catalog revision is required")
		}
		revision := string(value.Revision())
		if _, exists := result.catalogs[revision]; exists {
			return StaticCatalogRegistry{}, fmt.Errorf("catalog revision %q is registered twice", revision)
		}
		result.catalogs[revision] = value
	}
	return result, nil
}

func (registry StaticCatalogRegistry) Current() compilerpkg.NodeCatalog {
	return registry.catalogs[registry.current]
}

func (registry StaticCatalogRegistry) Get(revision string) (compilerpkg.NodeCatalog, bool) {
	value, exists := registry.catalogs[revision]
	return value, exists
}

func NewService(repository Repository, accessControl AccessControl, resourceResolver ResourceResolver, valueCompiler Compiler, catalogs CatalogRegistry, policy compilerpkg.Policy, ids identity.Generator) (Service, error) {
	if repository == nil || accessControl == nil || resourceResolver == nil || valueCompiler == nil || catalogs == nil || ids == nil {
		return Service{}, fmt.Errorf("definition service dependencies are required")
	}
	if catalogs.Current() == nil || catalogs.Current().Revision() == "" || policy.Revision() == "" {
		return Service{}, fmt.Errorf("definition service requires immutable catalog and policy revisions")
	}
	return Service{repository: repository, access: accessControl, resources: resourceResolver, compiler: valueCompiler, catalogs: catalogs, policy: policy, ids: ids, parser: ir.DefaultParser()}, nil
}

func NewBuiltinService(repository Repository, accessControl AccessControl, resourceResolver ResourceResolver) Service {
	registry, err := NewStaticCatalogRegistry(catalog.BuiltinV2(), catalog.BuiltinV1())
	if err != nil {
		panic(err)
	}
	service, err := NewService(repository, accessControl, resourceResolver, compilerpkg.BuiltinV2(), registry, compilerpkg.DefaultPolicyV1(), identity.UUIDv7Generator{})
	if err != nil {
		panic(err)
	}
	return service
}

func (service Service) catalogRevision(irVersion string) string {
	if irVersion == ir.VersionV1 {
		if value, exists := service.catalogs.Get(string(catalog.BuiltinRevisionV1)); exists {
			return string(value.Revision())
		}
	}
	if irVersion == ir.VersionV2 {
		if value, exists := service.catalogs.Get(string(catalog.BuiltinRevisionV2)); exists {
			return string(value.Revision())
		}
	}
	return string(service.catalogs.Current().Revision())
}

type CreateWorkflowCommand struct {
	ProjectID      string
	PrincipalID    string
	Name           string
	IRJSON         []byte
	IdempotencyKey string
}

func (service Service) CreateWorkflow(ctx context.Context, command CreateWorkflowCommand) (Workflow, DraftRevision, []ir.Diagnostic, error) {
	if err := service.access.Require(ctx, command.ProjectID, command.PrincipalID, access.PermissionWorkflowWrite); err != nil {
		return Workflow{}, DraftRevision{}, nil, mapAccessError(err)
	}
	document, canonical, diagnostics := service.parseDraft(command.IRJSON)
	if ir.HasErrors(diagnostics) {
		return Workflow{}, DraftRevision{}, diagnostics, nil
	}
	if err := validateMutation(command.ProjectID, command.PrincipalID, command.IdempotencyKey); err != nil || strings.TrimSpace(command.Name) == "" || len(command.Name) > 200 {
		return Workflow{}, DraftRevision{}, nil, invalidArgument("project_id, principal_id, idempotency key, and a bounded name are required")
	}
	workflowID, draftRevisionID, err := service.twoIDs()
	if err != nil {
		return Workflow{}, DraftRevision{}, nil, err
	}
	hash, err := requestHash(struct {
		Name string          `json:"name"`
		IR   json.RawMessage `json:"ir"`
	}{command.Name, canonical})
	if err != nil {
		return Workflow{}, DraftRevision{}, nil, err
	}
	workflow, revision, err := service.repository.CreateWorkflow(ctx, CreateWorkflowRecord{
		WorkflowID: workflowID, DraftRevisionID: draftRevisionID, ProjectID: command.ProjectID, Name: command.Name,
		IRJSON: canonical, CatalogRevision: service.catalogRevision(document.IRVersion), PrincipalID: command.PrincipalID,
		IdempotencyKey: command.IdempotencyKey, RequestHash: hash,
	})
	if err != nil {
		return Workflow{}, DraftRevision{}, nil, repositoryError("create workflow", err)
	}
	return workflow, revision, nil, nil
}

func (service Service) GetWorkflow(ctx context.Context, projectID, principalID, workflowID string) (Workflow, error) {
	if err := service.access.Require(ctx, projectID, principalID, access.PermissionWorkflowRead); err != nil {
		return Workflow{}, mapAccessError(err)
	}
	workflow, err := service.repository.GetWorkflow(ctx, projectID, workflowID)
	if err != nil {
		return Workflow{}, repositoryError("get workflow", err)
	}
	return workflow, nil
}

func (service Service) GetDraft(ctx context.Context, projectID, principalID, workflowID string) (Draft, error) {
	if err := service.access.Require(ctx, projectID, principalID, access.PermissionWorkflowRead); err != nil {
		return Draft{}, mapAccessError(err)
	}
	draft, err := service.repository.GetCurrentDraft(ctx, projectID, workflowID)
	if err != nil {
		return Draft{}, repositoryError("get draft", err)
	}
	return draft, nil
}

type SaveDraftCommand struct {
	ProjectID        string
	PrincipalID      string
	WorkflowID       string
	ExpectedRevision int64
	IRJSON           []byte
	IdempotencyKey   string
}

func (service Service) SaveDraft(ctx context.Context, command SaveDraftCommand) (DraftRevision, []ir.Diagnostic, error) {
	if err := service.access.Require(ctx, command.ProjectID, command.PrincipalID, access.PermissionWorkflowWrite); err != nil {
		return DraftRevision{}, nil, mapAccessError(err)
	}
	document, canonical, diagnostics := service.parseDraft(command.IRJSON)
	if ir.HasErrors(diagnostics) {
		return DraftRevision{}, diagnostics, nil
	}
	if err := validateMutation(command.ProjectID, command.PrincipalID, command.IdempotencyKey); err != nil || command.WorkflowID == "" || command.ExpectedRevision < 1 {
		return DraftRevision{}, nil, invalidArgument("workflow_id, positive expected_revision, and idempotency key are required")
	}
	baseRevision, err := service.repository.GetDraftRevision(ctx, command.ProjectID, command.WorkflowID, command.ExpectedRevision)
	if err != nil {
		return DraftRevision{}, nil, repositoryError("get draft base revision", err)
	}
	revisionID, err := service.ids.New()
	if err != nil {
		return DraftRevision{}, nil, err
	}
	hash, err := requestHash(struct {
		WorkflowID string          `json:"workflow_id"`
		Expected   int64           `json:"expected_revision"`
		IR         json.RawMessage `json:"ir"`
	}{command.WorkflowID, command.ExpectedRevision, canonical})
	if err != nil {
		return DraftRevision{}, nil, err
	}
	revision, err := service.repository.SaveDraft(ctx, SaveDraftRecord{
		DraftRevisionID: revisionID, ProjectID: command.ProjectID, WorkflowID: command.WorkflowID,
		ExpectedRevision: command.ExpectedRevision, IRJSON: canonical, CatalogRevision: service.catalogRevision(document.IRVersion),
		PrincipalID: command.PrincipalID, IdempotencyKey: command.IdempotencyKey, RequestHash: hash,
	})
	if err != nil {
		if errors.Is(err, ErrDraftRevisionConflict) {
			latest, readErr := service.repository.GetCurrentDraft(ctx, command.ProjectID, command.WorkflowID)
			if readErr == nil {
				return DraftRevision{}, nil, wrapError(CodeDraftRevisionConflict, "draft revision has changed", err, map[string]any{
					"base_revision": baseRevision.RevisionNumber, "current_revision": latest.CurrentRevision,
					"base_ir": baseRevision.IRJSON, "current_ir": latest.Current.IRJSON, "submitted_ir": json.RawMessage(canonical),
				})
			}
		}
		return DraftRevision{}, nil, repositoryError("save draft", err)
	}
	return revision, nil, nil
}

func (service Service) ValidateDraft(ctx context.Context, projectID, principalID, workflowID string, revisionNumber int64) ([]ir.Diagnostic, error) {
	if err := service.access.Require(ctx, projectID, principalID, access.PermissionWorkflowWrite); err != nil {
		return nil, mapAccessError(err)
	}
	_, diagnostics, err := service.compileRevision(ctx, projectID, principalID, workflowID, revisionNumber, resources.PurposeTest)
	return diagnostics, err
}

func (service Service) CompileDraftTestSnapshot(ctx context.Context, projectID, principalID, workflowID string, revisionNumber int64) (ExecutionSnapshot, []ir.Diagnostic, error) {
	if err := service.access.Require(ctx, projectID, principalID, access.PermissionWorkflowTest); err != nil {
		return ExecutionSnapshot{}, nil, mapAccessError(err)
	}
	compiled, diagnostics, err := service.compileRevision(ctx, projectID, principalID, workflowID, revisionNumber, resources.PurposeTest)
	if err != nil || ir.HasErrors(diagnostics) {
		return ExecutionSnapshot{}, diagnostics, err
	}
	snapshotID, err := service.ids.New()
	if err != nil {
		return ExecutionSnapshot{}, nil, err
	}
	snapshot, err := compiled.snapshot(snapshotID, SnapshotOriginDraftTest, compiled.revision.ID)
	if err != nil {
		return ExecutionSnapshot{}, nil, err
	}
	stored, err := service.repository.StoreTestSnapshot(ctx, snapshot)
	if err != nil {
		return ExecutionSnapshot{}, nil, repositoryError("store test snapshot", err)
	}
	return stored, nil, nil
}

// ResolveDraftTestSnapshot returns the immutable Snapshot that was compiled
// from a specific immutable Draft Revision. It never recompiles or follows the
// mutable draft pointer, so TestDraft Run creation can be retried safely.
func (service Service) ResolveDraftTestSnapshot(ctx context.Context, projectID, principalID, workflowID string, revisionNumber int64) (ExecutionSnapshot, error) {
	if err := service.access.Require(ctx, projectID, principalID, access.PermissionWorkflowTest); err != nil {
		return ExecutionSnapshot{}, mapAccessError(err)
	}
	if projectID == "" || workflowID == "" || revisionNumber < 1 {
		return ExecutionSnapshot{}, invalidArgument("project_id, workflow_id and positive revision are required")
	}
	snapshot, err := service.repository.ResolveDraftTestSnapshot(ctx, projectID, workflowID, revisionNumber)
	if err != nil {
		return ExecutionSnapshot{}, repositoryError("resolve draft test snapshot", err)
	}
	return snapshot, nil
}

type PublishCommand struct {
	ProjectID        string
	PrincipalID      string
	WorkflowID       string
	ExpectedRevision int64
	ChangeLog        string
	IdempotencyKey   string
}

func (service Service) Publish(ctx context.Context, command PublishCommand) (PublishedVersion, ExecutionSnapshot, []ir.Diagnostic, error) {
	if err := service.access.Require(ctx, command.ProjectID, command.PrincipalID, access.PermissionWorkflowPublish); err != nil {
		return PublishedVersion{}, ExecutionSnapshot{}, nil, mapAccessError(err)
	}
	if err := validateMutation(command.ProjectID, command.PrincipalID, command.IdempotencyKey); err != nil || command.WorkflowID == "" || command.ExpectedRevision < 1 || len(command.ChangeLog) > 4000 {
		return PublishedVersion{}, ExecutionSnapshot{}, nil, invalidArgument("workflow_id, positive expected_revision, bounded change_log, and idempotency key are required")
	}
	hash, err := requestHash(struct {
		WorkflowID string `json:"workflow_id"`
		Revision   int64  `json:"revision"`
		ChangeLog  string `json:"change_log"`
	}{command.WorkflowID, command.ExpectedRevision, command.ChangeLog})
	if err != nil {
		return PublishedVersion{}, ExecutionSnapshot{}, nil, err
	}
	replayedVersion, replayedSnapshot, exists, err := service.repository.FindPublishedByIdempotency(
		ctx, command.ProjectID, command.WorkflowID, command.IdempotencyKey, hash,
	)
	if err != nil {
		return PublishedVersion{}, ExecutionSnapshot{}, nil, repositoryError("find published idempotency result", err)
	}
	if exists {
		return replayedVersion, replayedSnapshot, nil, nil
	}
	compiled, diagnostics, err := service.compileRevision(ctx, command.ProjectID, command.PrincipalID, command.WorkflowID, command.ExpectedRevision, resources.PurposeProduction)
	if err != nil || ir.HasErrors(diagnostics) {
		return PublishedVersion{}, ExecutionSnapshot{}, diagnostics, err
	}
	versionID, snapshotID, err := service.twoIDs()
	if err != nil {
		return PublishedVersion{}, ExecutionSnapshot{}, nil, err
	}
	snapshot, err := compiled.snapshot(snapshotID, SnapshotOriginPublished, versionID)
	if err != nil {
		return PublishedVersion{}, ExecutionSnapshot{}, nil, err
	}
	auditID, err := service.ids.New()
	if err != nil {
		return PublishedVersion{}, ExecutionSnapshot{}, nil, err
	}
	version, stored, err := service.repository.Publish(ctx, PublishRecord{
		VersionID: versionID, AuditID: auditID, ProjectID: command.ProjectID, WorkflowID: command.WorkflowID,
		ExpectedRevision: command.ExpectedRevision, DraftRevisionID: compiled.revision.ID, ChangeLog: command.ChangeLog,
		PrincipalID: command.PrincipalID, IdempotencyKey: command.IdempotencyKey, RequestHash: hash, Snapshot: snapshot,
	})
	if err != nil {
		return PublishedVersion{}, ExecutionSnapshot{}, nil, repositoryError("publish workflow", err)
	}
	return version, stored, nil, nil
}

func (service Service) Rollback(ctx context.Context, projectID, principalID, workflowID string, versionNumber int64) (PublishedVersion, error) {
	if err := service.access.Require(ctx, projectID, principalID, access.PermissionWorkflowPublish); err != nil {
		return PublishedVersion{}, mapAccessError(err)
	}
	auditID, err := service.ids.New()
	if err != nil {
		return PublishedVersion{}, err
	}
	version, err := service.repository.Rollback(ctx, RollbackRecord{AuditID: auditID, ProjectID: projectID, WorkflowID: workflowID, VersionNumber: versionNumber, PrincipalID: principalID})
	if err != nil {
		return PublishedVersion{}, repositoryError("rollback workflow", err)
	}
	return version, nil
}

type CopyCommand struct {
	ProjectID           string
	PrincipalID         string
	SourceWorkflowID    string
	SourceVersionNumber int64
	Name                string
	IdempotencyKey      string
}

func (service Service) CopyPublishedVersion(ctx context.Context, command CopyCommand) (Workflow, DraftRevision, error) {
	for _, permission := range []access.Permission{access.PermissionWorkflowRead, access.PermissionWorkflowWrite} {
		if err := service.access.Require(ctx, command.ProjectID, command.PrincipalID, permission); err != nil {
			return Workflow{}, DraftRevision{}, mapAccessError(err)
		}
	}
	if err := validateMutation(command.ProjectID, command.PrincipalID, command.IdempotencyKey); err != nil || command.SourceWorkflowID == "" || command.SourceVersionNumber < 1 || strings.TrimSpace(command.Name) == "" || len(command.Name) > 200 {
		return Workflow{}, DraftRevision{}, invalidArgument("source workflow/version, bounded name, and idempotency key are required")
	}
	workflowID, revisionID, err := service.twoIDs()
	if err != nil {
		return Workflow{}, DraftRevision{}, err
	}
	hash, err := requestHash(struct {
		SourceWorkflowID    string `json:"source_workflow_id"`
		SourceVersionNumber int64  `json:"source_version_number"`
		Name                string `json:"name"`
	}{command.SourceWorkflowID, command.SourceVersionNumber, command.Name})
	if err != nil {
		return Workflow{}, DraftRevision{}, err
	}
	workflow, revision, err := service.repository.CopyPublishedVersion(ctx, CopyRecord{
		WorkflowID: workflowID, DraftRevisionID: revisionID, ProjectID: command.ProjectID,
		SourceWorkflowID: command.SourceWorkflowID, SourceVersionNumber: command.SourceVersionNumber,
		Name: command.Name, PrincipalID: command.PrincipalID, IdempotencyKey: command.IdempotencyKey, RequestHash: hash,
	})
	if err != nil {
		return Workflow{}, DraftRevision{}, repositoryError("copy published version", err)
	}
	return workflow, revision, nil
}

func (service Service) ResolveProductionDefinition(ctx context.Context, projectID, principalID, workflowID string) (ProductionDefinition, error) {
	if err := service.access.Require(ctx, projectID, principalID, access.PermissionRunCreate); err != nil {
		return ProductionDefinition{}, mapAccessError(err)
	}
	result, err := service.repository.ResolveProductionDefinition(ctx, projectID, workflowID)
	if err != nil {
		return ProductionDefinition{}, repositoryError("resolve production definition", err)
	}
	return result, nil
}

type compiledRevision struct {
	revision DraftRevision
	result   compilerpkg.Result
}

func (service Service) compileRevision(ctx context.Context, projectID, principalID, workflowID string, revisionNumber int64, purpose resources.Purpose) (compiledRevision, []ir.Diagnostic, error) {
	revision, err := service.repository.GetDraftRevision(ctx, projectID, workflowID, revisionNumber)
	if err != nil {
		return compiledRevision{}, nil, repositoryError("get draft revision", err)
	}
	resolvedCatalog, exists := service.catalogs.Get(revision.CatalogRevision)
	if !exists {
		return compiledRevision{}, nil, wrapError(CodeCatalogUnavailable, "draft catalog revision is unavailable", nil, map[string]any{"catalog_revision": revision.CatalogRevision})
	}
	document, diagnostics := service.parser.ParseDraft(revision.IRJSON)
	if ir.HasErrors(diagnostics) {
		return compiledRevision{}, diagnostics, nil
	}
	bindings, diagnostics, err := service.resources.Resolve(ctx, projectID, principalID, purpose, document)
	if err != nil {
		return compiledRevision{}, nil, mapAccessError(err)
	}
	if ir.HasErrors(diagnostics) {
		return compiledRevision{}, diagnostics, nil
	}
	result, diagnostics := service.compiler.Compile(compilerpkg.Request{IR: document, Catalog: resolvedCatalog, Policy: service.policy, Resources: bindings})
	if ir.HasErrors(diagnostics) {
		return compiledRevision{}, diagnostics, nil
	}
	return compiledRevision{revision: revision, result: result}, nil, nil
}

func (value compiledRevision) snapshot(id string, origin SnapshotOrigin, originID string) (ExecutionSnapshot, error) {
	manifest, err := json.Marshal(value.result.Manifest)
	if err != nil {
		return ExecutionSnapshot{}, fmt.Errorf("encode compiler manifest: %w", err)
	}
	manifest, err = ir.CanonicalizeJSON(manifest, ir.DefaultParseLimits)
	if err != nil {
		return ExecutionSnapshot{}, fmt.Errorf("canonicalize compiler manifest: %w", err)
	}
	return ExecutionSnapshot{
		ID: id, ProjectID: value.revision.ProjectID, WorkflowID: value.revision.WorkflowID, OriginKind: origin, OriginID: originID,
		IRJSON: value.result.CanonicalIR, DSLJSON: value.result.CanonicalDSL, SourceMapJSON: value.result.CanonicalSourceMap, ManifestJSON: manifest,
		IRHash: value.result.Hashes.IRHash, DSLHash: value.result.Hashes.DSLHash, SourceMapHash: value.result.Hashes.SourceMapHash, DefinitionHash: value.result.Hashes.DefinitionHash,
	}, nil
}

func (service Service) parseDraft(raw []byte) (ir.Document, []byte, []ir.Diagnostic) {
	document, diagnostics := service.parser.ParseDraft(raw)
	if ir.HasErrors(diagnostics) {
		return ir.Document{}, nil, diagnostics
	}
	canonical, err := ir.CanonicalizeJSON(raw, ir.DefaultParseLimits)
	if err != nil {
		return ir.Document{}, nil, []ir.Diagnostic{ir.ErrorDiagnostic(ir.PhaseCanonical, "IR_CANONICALIZATION_FAILED", err.Error(), ir.Location{})}
	}
	return document, canonical, nil
}

func requestHash(value any) (string, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return "", fmt.Errorf("encode idempotent request: %w", err)
	}
	canonical, err := ir.CanonicalizeJSON(raw, ir.DefaultParseLimits)
	if err != nil {
		return "", fmt.Errorf("canonicalize idempotent request: %w", err)
	}
	return ir.HashCanonical(canonical), nil
}

func validateMutation(projectID, principalID, idempotencyKey string) error {
	if projectID == "" || principalID == "" || len(idempotencyKey) < 8 || len(idempotencyKey) > 200 || strings.TrimSpace(idempotencyKey) != idempotencyKey {
		return errors.New("invalid mutation identity")
	}
	return nil
}

func (service Service) twoIDs() (string, string, error) {
	first, err := service.ids.New()
	if err != nil {
		return "", "", err
	}
	second, err := service.ids.New()
	if err != nil {
		return "", "", err
	}
	return first, second, nil
}

func mapAccessError(err error) error {
	switch {
	case errors.Is(err, access.ErrPermissionDenied):
		return wrapError(CodePermissionDenied, "permission denied", err, nil)
	case errors.Is(err, access.ErrResourceNotFound), errors.Is(err, resources.ErrResourceNotFound):
		return wrapError(CodeResourceNotFound, "resource was not found", err, nil)
	default:
		return err
	}
}

func invalidArgument(message string) error {
	return wrapError(CodeInvalidArgument, message, nil, nil)
}
