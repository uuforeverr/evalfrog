package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	"github.com/uu999/evalfrog/internal/definition"
)

type scanner interface {
	Scan(...any) error
}

type idempotencyResult struct {
	requestHash string
	responseID  string
	auxiliaryID *string
}

func (store *Store) CreateWorkflow(ctx context.Context, record definition.CreateWorkflowRecord) (definition.Workflow, definition.DraftRevision, error) {
	tx, err := store.pool.Begin(ctx)
	if err != nil {
		return definition.Workflow{}, definition.DraftRevision{}, err
	}
	defer tx.Rollback(ctx)
	result, exists, err := claimIdempotency(ctx, tx, record.ProjectID, "create_workflow", "project", record.IdempotencyKey,
		record.RequestHash, "workflow", record.WorkflowID, &record.DraftRevisionID)
	if err != nil {
		return definition.Workflow{}, definition.DraftRevision{}, err
	}
	if exists {
		if result.requestHash != record.RequestHash {
			return definition.Workflow{}, definition.DraftRevision{}, definition.ErrIdempotencyKeyReused
		}
		workflow, err := getWorkflow(ctx, tx, record.ProjectID, result.responseID, false)
		if err != nil {
			return definition.Workflow{}, definition.DraftRevision{}, err
		}
		if result.auxiliaryID == nil {
			return definition.Workflow{}, definition.DraftRevision{}, fmt.Errorf("create workflow idempotency record has no draft revision")
		}
		revision, err := getDraftRevisionByID(ctx, tx, record.ProjectID, result.responseID, *result.auxiliaryID)
		return workflow, revision, err
	}
	if _, err = tx.Exec(ctx, `
		INSERT INTO workflows (workflow_id, project_id, name, created_by)
		VALUES (UUID_TO_BIN($1), UUID_TO_BIN($2), $3, UUID_TO_BIN($4))`, record.WorkflowID, record.ProjectID, record.Name, record.PrincipalID); err != nil {
		return definition.Workflow{}, definition.DraftRevision{}, err
	}
	if err = insertDraftRevision(ctx, tx, definition.SaveDraftRecord{
		DraftRevisionID: record.DraftRevisionID, ProjectID: record.ProjectID, WorkflowID: record.WorkflowID,
		IRJSON: record.IRJSON, CatalogRevision: record.CatalogRevision, PrincipalID: record.PrincipalID,
	}, 1, nil); err != nil {
		return definition.Workflow{}, definition.DraftRevision{}, err
	}
	if _, err = tx.Exec(ctx, `
		INSERT INTO workflow_drafts (project_id, workflow_id, current_revision, current_revision_id, state_version)
		VALUES (UUID_TO_BIN($1), UUID_TO_BIN($2), 1, UUID_TO_BIN($3), 1)`, record.ProjectID, record.WorkflowID, record.DraftRevisionID); err != nil {
		return definition.Workflow{}, definition.DraftRevision{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return definition.Workflow{}, definition.DraftRevision{}, err
	}
	return store.GetWorkflowAndRevision(ctx, record.ProjectID, record.WorkflowID, record.DraftRevisionID)
}

func (store *Store) GetWorkflowAndRevision(ctx context.Context, projectID, workflowID, revisionID string) (definition.Workflow, definition.DraftRevision, error) {
	workflow, err := store.GetWorkflow(ctx, projectID, workflowID)
	if err != nil {
		return definition.Workflow{}, definition.DraftRevision{}, err
	}
	revision, err := getDraftRevisionByID(ctx, store.pool, projectID, workflowID, revisionID)
	return workflow, revision, err
}

func (store *Store) GetWorkflow(ctx context.Context, projectID, workflowID string) (definition.Workflow, error) {
	return getWorkflow(ctx, store.pool, projectID, workflowID, false)
}

func (store *Store) GetCurrentDraft(ctx context.Context, projectID, workflowID string) (definition.Draft, error) {
	row := store.pool.QueryRow(ctx, `
		SELECT BIN_TO_UUID(d.project_id), BIN_TO_UUID(d.workflow_id), d.current_revision, d.state_version,
		       BIN_TO_UUID(r.draft_revision_id), BIN_TO_UUID(r.project_id), BIN_TO_UUID(r.workflow_id),
		       r.revision_number, r.ir_json, r.catalog_revision,
		       BIN_TO_UUID(r.created_by), r.created_at, BIN_TO_UUID(r.cloned_from_version_id)
		FROM workflow_drafts d
		JOIN workflow_draft_revisions r
		  ON r.project_id = d.project_id AND r.workflow_id = d.workflow_id
		 AND r.draft_revision_id = d.current_revision_id
		WHERE d.project_id = UUID_TO_BIN($1) AND d.workflow_id = UUID_TO_BIN($2)`, projectID, workflowID)
	var draft definition.Draft
	err := row.Scan(&draft.ProjectID, &draft.WorkflowID, &draft.CurrentRevision, &draft.StateVersion,
		&draft.Current.ID, &draft.Current.ProjectID, &draft.Current.WorkflowID, &draft.Current.RevisionNumber,
		&draft.Current.IRJSON, &draft.Current.CatalogRevision, &draft.Current.CreatedBy,
		&draft.Current.CreatedAt, &draft.Current.ClonedFromVersionID)
	if errors.Is(err, sql.ErrNoRows) {
		return definition.Draft{}, definition.ErrResourceNotFound
	}
	return draft, err
}

func (store *Store) GetDraftRevision(ctx context.Context, projectID, workflowID string, revisionNumber int64) (definition.DraftRevision, error) {
	return scanDraftRevision(store.pool.QueryRow(ctx, draftRevisionSelect+`
		WHERE project_id = UUID_TO_BIN($1) AND workflow_id = UUID_TO_BIN($2) AND revision_number = $3`, projectID, workflowID, revisionNumber))
}

func (store *Store) SaveDraft(ctx context.Context, record definition.SaveDraftRecord) (definition.DraftRevision, error) {
	tx, err := store.pool.Begin(ctx)
	if err != nil {
		return definition.DraftRevision{}, err
	}
	defer tx.Rollback(ctx)
	scope := record.WorkflowID
	result, exists, err := claimIdempotency(ctx, tx, record.ProjectID, "save_draft", scope, record.IdempotencyKey,
		record.RequestHash, "draft_revision", record.DraftRevisionID, nil)
	if err != nil {
		return definition.DraftRevision{}, err
	}
	if exists {
		if result.requestHash != record.RequestHash {
			return definition.DraftRevision{}, definition.ErrIdempotencyKeyReused
		}
		return getDraftRevisionByID(ctx, tx, record.ProjectID, record.WorkflowID, result.responseID)
	}
	var current int64
	err = tx.QueryRow(ctx, `
		SELECT current_revision FROM workflow_drafts
		WHERE project_id = UUID_TO_BIN($1) AND workflow_id = UUID_TO_BIN($2)
		FOR UPDATE`, record.ProjectID, record.WorkflowID).Scan(&current)
	if errors.Is(err, sql.ErrNoRows) {
		return definition.DraftRevision{}, definition.ErrResourceNotFound
	}
	if err != nil {
		return definition.DraftRevision{}, err
	}
	if current != record.ExpectedRevision {
		return definition.DraftRevision{}, definition.ErrDraftRevisionConflict
	}
	next := current + 1
	if err = insertDraftRevision(ctx, tx, record, next, nil); err != nil {
		return definition.DraftRevision{}, err
	}
	command, err := tx.Exec(ctx, `
		UPDATE workflow_drafts
		SET current_revision = $3, current_revision_id = UUID_TO_BIN($4),
		    state_version = state_version + 1, updated_at = CURRENT_TIMESTAMP(6)
		WHERE project_id = UUID_TO_BIN($1) AND workflow_id = UUID_TO_BIN($2) AND current_revision = $5`,
		record.ProjectID, record.WorkflowID, next, record.DraftRevisionID, record.ExpectedRevision)
	if err != nil {
		return definition.DraftRevision{}, err
	}
	if command.RowsAffected() != 1 {
		return definition.DraftRevision{}, definition.ErrDraftRevisionConflict
	}
	if _, err = tx.Exec(ctx, `UPDATE workflows SET updated_at = CURRENT_TIMESTAMP(6) WHERE project_id = UUID_TO_BIN($1) AND workflow_id = UUID_TO_BIN($2)`, record.ProjectID, record.WorkflowID); err != nil {
		return definition.DraftRevision{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return definition.DraftRevision{}, err
	}
	return getDraftRevisionByID(ctx, store.pool, record.ProjectID, record.WorkflowID, record.DraftRevisionID)
}

func (store *Store) StoreTestSnapshot(ctx context.Context, snapshot definition.ExecutionSnapshot) (definition.ExecutionSnapshot, error) {
	tx, err := store.pool.Begin(ctx)
	if err != nil {
		return definition.ExecutionSnapshot{}, err
	}
	defer tx.Rollback(ctx)
	stored, err := upsertSnapshot(ctx, tx, snapshot)
	if err != nil {
		return definition.ExecutionSnapshot{}, err
	}
	if snapshot.OriginKind != definition.SnapshotOriginDraftTest || snapshot.OriginID == "" {
		return definition.ExecutionSnapshot{}, fmt.Errorf("test snapshot requires its immutable draft revision identity")
	}
	if _, err = tx.Exec(ctx, `
		INSERT INTO workflow_draft_test_snapshots (
			project_id, workflow_id, draft_revision_id, snapshot_id
		) VALUES (UUID_TO_BIN($1),UUID_TO_BIN($2),UUID_TO_BIN($3),UUID_TO_BIN($4))`, snapshot.ProjectID, snapshot.WorkflowID, snapshot.OriginID, stored.ID); err != nil {
		if !isDuplicateKey(err) {
			return definition.ExecutionSnapshot{}, err
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return definition.ExecutionSnapshot{}, err
	}
	return stored, nil
}

func (store *Store) ResolveDraftTestSnapshot(ctx context.Context, projectID, workflowID string, revisionNumber int64) (definition.ExecutionSnapshot, error) {
	return scanSnapshot(store.pool.QueryRow(ctx, `
		SELECT BIN_TO_UUID(s.snapshot_id), BIN_TO_UUID(s.project_id), BIN_TO_UUID(s.workflow_id), s.origin_kind,
		       BIN_TO_UUID(s.origin_id), s.ir_json, s.dsl_json, s.source_map_json, s.compiler_manifest_json,
		       s.ir_hash, s.dsl_hash, s.source_map_hash, s.definition_hash, s.created_at
		FROM workflow_draft_revisions r
		JOIN workflow_draft_test_snapshots b
		  ON b.project_id=r.project_id AND b.workflow_id=r.workflow_id
		 AND b.draft_revision_id=r.draft_revision_id
		JOIN workflow_execution_snapshots s
		  ON s.project_id=b.project_id AND s.workflow_id=b.workflow_id AND s.snapshot_id=b.snapshot_id
		WHERE s.project_id=UUID_TO_BIN($1) AND s.workflow_id=UUID_TO_BIN($2)
		  AND r.revision_number=$3
		ORDER BY b.created_at DESC, s.snapshot_id DESC
		LIMIT 1`, projectID, workflowID, revisionNumber))
}

func (store *Store) FindPublishedByIdempotency(ctx context.Context, projectID, workflowID, key, requestHash string) (definition.PublishedVersion, definition.ExecutionSnapshot, bool, error) {
	var storedHash, versionID string
	err := store.pool.QueryRow(ctx, `
		SELECT request_hash, BIN_TO_UUID(response_id)
		FROM definition_idempotency
		WHERE project_id = UUID_TO_BIN($1) AND command_name = 'publish_workflow'
		  AND target_scope = $2 AND idempotency_key = $3`,
		projectID, workflowID, key).Scan(&storedHash, &versionID)
	if errors.Is(err, sql.ErrNoRows) {
		return definition.PublishedVersion{}, definition.ExecutionSnapshot{}, false, nil
	}
	if err != nil {
		return definition.PublishedVersion{}, definition.ExecutionSnapshot{}, false, err
	}
	if storedHash != requestHash {
		return definition.PublishedVersion{}, definition.ExecutionSnapshot{}, false, definition.ErrIdempotencyKeyReused
	}
	version, snapshot, err := getVersionAndSnapshotByID(ctx, store.pool, projectID, workflowID, versionID)
	return version, snapshot, err == nil, err
}

func (store *Store) Publish(ctx context.Context, record definition.PublishRecord) (definition.PublishedVersion, definition.ExecutionSnapshot, error) {
	tx, err := store.pool.Begin(ctx)
	if err != nil {
		return definition.PublishedVersion{}, definition.ExecutionSnapshot{}, err
	}
	defer tx.Rollback(ctx)
	result, exists, err := claimIdempotency(ctx, tx, record.ProjectID, "publish_workflow", record.WorkflowID, record.IdempotencyKey,
		record.RequestHash, "published_version", record.VersionID, &record.Snapshot.ID)
	if err != nil {
		return definition.PublishedVersion{}, definition.ExecutionSnapshot{}, err
	}
	if exists {
		if result.requestHash != record.RequestHash {
			return definition.PublishedVersion{}, definition.ExecutionSnapshot{}, definition.ErrIdempotencyKeyReused
		}
		version, snapshot, err := getVersionAndSnapshotByID(ctx, tx, record.ProjectID, record.WorkflowID, result.responseID)
		return version, snapshot, err
	}
	var currentRevision, versionNumber int64
	err = tx.QueryRow(ctx, `
		SELECT d.current_revision, w.next_version_number
		FROM workflow_drafts d
		JOIN workflows w ON w.project_id = d.project_id AND w.workflow_id = d.workflow_id
		WHERE d.project_id = UUID_TO_BIN($1) AND d.workflow_id = UUID_TO_BIN($2)
		FOR UPDATE`, record.ProjectID, record.WorkflowID).Scan(&currentRevision, &versionNumber)
	if errors.Is(err, sql.ErrNoRows) {
		return definition.PublishedVersion{}, definition.ExecutionSnapshot{}, definition.ErrResourceNotFound
	}
	if err != nil {
		return definition.PublishedVersion{}, definition.ExecutionSnapshot{}, err
	}
	if currentRevision != record.ExpectedRevision {
		return definition.PublishedVersion{}, definition.ExecutionSnapshot{}, definition.ErrDraftRevisionConflict
	}
	var actualDraftID string
	if err = tx.QueryRow(ctx, `
		SELECT BIN_TO_UUID(draft_revision_id) FROM workflow_draft_revisions
		WHERE project_id = UUID_TO_BIN($1) AND workflow_id = UUID_TO_BIN($2) AND revision_number = $3`,
		record.ProjectID, record.WorkflowID, record.ExpectedRevision).Scan(&actualDraftID); err != nil {
		return definition.PublishedVersion{}, definition.ExecutionSnapshot{}, err
	}
	if actualDraftID != record.DraftRevisionID {
		return definition.PublishedVersion{}, definition.ExecutionSnapshot{}, definition.ErrDraftRevisionConflict
	}
	stored, err := upsertSnapshot(ctx, tx, record.Snapshot)
	if err != nil {
		return definition.PublishedVersion{}, definition.ExecutionSnapshot{}, err
	}
	if stored.ID != record.Snapshot.ID {
		if _, err = tx.Exec(ctx, `
			UPDATE definition_idempotency SET response_aux_id=UUID_TO_BIN($1)
			WHERE project_id=UUID_TO_BIN($2) AND command_name='publish_workflow'
			  AND target_scope=$3 AND idempotency_key=$4`,
			stored.ID, record.ProjectID, record.WorkflowID, record.IdempotencyKey); err != nil {
			return definition.PublishedVersion{}, definition.ExecutionSnapshot{}, err
		}
	}
	if _, err = tx.Exec(ctx, `
		INSERT INTO workflow_versions (
			version_id, project_id, workflow_id, version_number,
			source_draft_revision_id, execution_snapshot_id, change_log, created_by
		) VALUES (UUID_TO_BIN($1),UUID_TO_BIN($2),UUID_TO_BIN($3),$4,UUID_TO_BIN($5),UUID_TO_BIN($6),$7,UUID_TO_BIN($8))`,
		record.VersionID, record.ProjectID, record.WorkflowID, versionNumber,
		record.DraftRevisionID, stored.ID, record.ChangeLog, record.PrincipalID); err != nil {
		return definition.PublishedVersion{}, definition.ExecutionSnapshot{}, err
	}
	command, err := tx.Exec(ctx, `
		UPDATE workflows SET active_version_id = UUID_TO_BIN($3), next_version_number = $4 + 1, updated_at = CURRENT_TIMESTAMP(6)
		WHERE project_id = UUID_TO_BIN($1) AND workflow_id = UUID_TO_BIN($2) AND next_version_number = $4`,
		record.ProjectID, record.WorkflowID, record.VersionID, versionNumber)
	if err != nil {
		return definition.PublishedVersion{}, definition.ExecutionSnapshot{}, err
	}
	if command.RowsAffected() != 1 {
		return definition.PublishedVersion{}, definition.ExecutionSnapshot{}, definition.ErrDraftRevisionConflict
	}
	if _, err = tx.Exec(ctx, `
		INSERT INTO workflow_definition_audits (audit_id, project_id, workflow_id, event_type, version_id, principal_id)
		VALUES (UUID_TO_BIN($1),UUID_TO_BIN($2),UUID_TO_BIN($3),'publish',UUID_TO_BIN($4),UUID_TO_BIN($5))`, record.AuditID, record.ProjectID, record.WorkflowID, record.VersionID, record.PrincipalID); err != nil {
		return definition.PublishedVersion{}, definition.ExecutionSnapshot{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return definition.PublishedVersion{}, definition.ExecutionSnapshot{}, err
	}
	version, _, err := getVersionAndSnapshotByID(ctx, store.pool, record.ProjectID, record.WorkflowID, record.VersionID)
	return version, stored, err
}

func (store *Store) Rollback(ctx context.Context, record definition.RollbackRecord) (definition.PublishedVersion, error) {
	tx, err := store.pool.Begin(ctx)
	if err != nil {
		return definition.PublishedVersion{}, err
	}
	defer tx.Rollback(ctx)
	var currentVersionID *string
	err = tx.QueryRow(ctx, `
		SELECT BIN_TO_UUID(active_version_id) FROM workflows
		WHERE project_id = UUID_TO_BIN($1) AND workflow_id = UUID_TO_BIN($2) FOR UPDATE`, record.ProjectID, record.WorkflowID).Scan(&currentVersionID)
	if errors.Is(err, sql.ErrNoRows) {
		return definition.PublishedVersion{}, definition.ErrResourceNotFound
	}
	if err != nil {
		return definition.PublishedVersion{}, err
	}
	version, err := scanVersion(tx.QueryRow(ctx, versionSelect+`
		WHERE project_id = UUID_TO_BIN($1) AND workflow_id = UUID_TO_BIN($2) AND version_number = $3`, record.ProjectID, record.WorkflowID, record.VersionNumber))
	if err != nil {
		return definition.PublishedVersion{}, err
	}
	if currentVersionID != nil && *currentVersionID == version.ID {
		return version, tx.Commit(ctx)
	}
	if _, err = tx.Exec(ctx, `UPDATE workflows SET active_version_id = UUID_TO_BIN($3), updated_at = CURRENT_TIMESTAMP(6) WHERE project_id = UUID_TO_BIN($1) AND workflow_id = UUID_TO_BIN($2)`, record.ProjectID, record.WorkflowID, version.ID); err != nil {
		return definition.PublishedVersion{}, err
	}
	if _, err = tx.Exec(ctx, `
		INSERT INTO workflow_definition_audits (audit_id, project_id, workflow_id, event_type, version_id, principal_id)
		VALUES (UUID_TO_BIN($1),UUID_TO_BIN($2),UUID_TO_BIN($3),'rollback',UUID_TO_BIN($4),UUID_TO_BIN($5))`, record.AuditID, record.ProjectID, record.WorkflowID, version.ID, record.PrincipalID); err != nil {
		return definition.PublishedVersion{}, err
	}
	return version, tx.Commit(ctx)
}

func (store *Store) CopyPublishedVersion(ctx context.Context, record definition.CopyRecord) (definition.Workflow, definition.DraftRevision, error) {
	tx, err := store.pool.Begin(ctx)
	if err != nil {
		return definition.Workflow{}, definition.DraftRevision{}, err
	}
	defer tx.Rollback(ctx)
	result, exists, err := claimIdempotency(ctx, tx, record.ProjectID, "copy_published_version", "project", record.IdempotencyKey,
		record.RequestHash, "workflow", record.WorkflowID, &record.DraftRevisionID)
	if err != nil {
		return definition.Workflow{}, definition.DraftRevision{}, err
	}
	if exists {
		if result.requestHash != record.RequestHash {
			return definition.Workflow{}, definition.DraftRevision{}, definition.ErrIdempotencyKeyReused
		}
		if result.auxiliaryID == nil {
			return definition.Workflow{}, definition.DraftRevision{}, fmt.Errorf("copy idempotency record has no draft revision")
		}
		workflow, err := getWorkflow(ctx, tx, record.ProjectID, result.responseID, false)
		if err != nil {
			return definition.Workflow{}, definition.DraftRevision{}, err
		}
		revision, err := getDraftRevisionByID(ctx, tx, record.ProjectID, result.responseID, *result.auxiliaryID)
		return workflow, revision, err
	}
	var sourceVersionID, catalogRevision string
	var irJSON json.RawMessage
	err = tx.QueryRow(ctx, `
		SELECT BIN_TO_UUID(v.version_id), s.ir_json,
		       JSON_UNQUOTE(JSON_EXTRACT(s.compiler_manifest_json, '$.catalog_revision'))
		FROM workflow_versions v
		JOIN workflow_execution_snapshots s
		  ON s.project_id = v.project_id AND s.workflow_id = v.workflow_id
		 AND s.snapshot_id = v.execution_snapshot_id
		WHERE v.project_id = UUID_TO_BIN($1) AND v.workflow_id = UUID_TO_BIN($2) AND v.version_number = $3`,
		record.ProjectID, record.SourceWorkflowID, record.SourceVersionNumber).Scan(&sourceVersionID, &irJSON, &catalogRevision)
	if errors.Is(err, sql.ErrNoRows) {
		return definition.Workflow{}, definition.DraftRevision{}, definition.ErrResourceNotFound
	}
	if err != nil {
		return definition.Workflow{}, definition.DraftRevision{}, err
	}
	if _, err = tx.Exec(ctx, `
		INSERT INTO workflows (workflow_id, project_id, name, cloned_from_version_id, created_by)
		VALUES (UUID_TO_BIN($1),UUID_TO_BIN($2),$3,UUID_TO_BIN($4),UUID_TO_BIN($5))`, record.WorkflowID, record.ProjectID, record.Name, sourceVersionID, record.PrincipalID); err != nil {
		return definition.Workflow{}, definition.DraftRevision{}, err
	}
	if err = insertDraftRevision(ctx, tx, definition.SaveDraftRecord{
		DraftRevisionID: record.DraftRevisionID, ProjectID: record.ProjectID, WorkflowID: record.WorkflowID,
		IRJSON: irJSON, CatalogRevision: catalogRevision, PrincipalID: record.PrincipalID,
	}, 1, &sourceVersionID); err != nil {
		return definition.Workflow{}, definition.DraftRevision{}, err
	}
	if _, err = tx.Exec(ctx, `
		INSERT INTO workflow_drafts (project_id, workflow_id, current_revision, current_revision_id, state_version)
		VALUES (UUID_TO_BIN($1),UUID_TO_BIN($2),1,UUID_TO_BIN($3),1)`, record.ProjectID, record.WorkflowID, record.DraftRevisionID); err != nil {
		return definition.Workflow{}, definition.DraftRevision{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return definition.Workflow{}, definition.DraftRevision{}, err
	}
	return store.GetWorkflowAndRevision(ctx, record.ProjectID, record.WorkflowID, record.DraftRevisionID)
}

func (store *Store) ResolveProductionDefinition(ctx context.Context, projectID, workflowID string) (definition.ProductionDefinition, error) {
	workflow, err := store.GetWorkflow(ctx, projectID, workflowID)
	if err != nil {
		return definition.ProductionDefinition{}, err
	}
	if workflow.ActiveVersionID == nil {
		return definition.ProductionDefinition{}, definition.ErrWorkflowNotPublished
	}
	version, snapshot, err := getVersionAndSnapshotByID(ctx, store.pool, projectID, workflowID, *workflow.ActiveVersionID)
	if err != nil {
		return definition.ProductionDefinition{}, err
	}
	return definition.ProductionDefinition{Workflow: workflow, Version: version, Snapshot: snapshot}, nil
}

func getWorkflow(ctx context.Context, queryer interface {
	QueryRow(context.Context, string, ...any) Row
}, projectID, workflowID string, forUpdate bool) (definition.Workflow, error) {
	query := `
		SELECT BIN_TO_UUID(workflow_id), BIN_TO_UUID(project_id), name, BIN_TO_UUID(active_version_id),
		       BIN_TO_UUID(created_by), created_at, updated_at
		FROM workflows WHERE project_id = UUID_TO_BIN($1) AND workflow_id = UUID_TO_BIN($2)`
	if forUpdate {
		query += " FOR UPDATE"
	}
	var result definition.Workflow
	err := queryer.QueryRow(ctx, query, projectID, workflowID).Scan(
		&result.ID, &result.ProjectID, &result.Name, &result.ActiveVersionID,
		&result.CreatedBy, &result.CreatedAt, &result.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return definition.Workflow{}, definition.ErrResourceNotFound
	}
	return result, err
}

const draftRevisionSelect = `
	SELECT BIN_TO_UUID(draft_revision_id), BIN_TO_UUID(project_id), BIN_TO_UUID(workflow_id),
	       revision_number, ir_json, catalog_revision, BIN_TO_UUID(created_by),
	       created_at, BIN_TO_UUID(cloned_from_version_id)
	FROM workflow_draft_revisions `

func scanDraftRevision(row scanner) (definition.DraftRevision, error) {
	var result definition.DraftRevision
	err := row.Scan(&result.ID, &result.ProjectID, &result.WorkflowID, &result.RevisionNumber,
		&result.IRJSON, &result.CatalogRevision, &result.CreatedBy, &result.CreatedAt, &result.ClonedFromVersionID)
	if errors.Is(err, sql.ErrNoRows) {
		return definition.DraftRevision{}, definition.ErrResourceNotFound
	}
	return result, err
}

func getDraftRevisionByID(ctx context.Context, queryer interface {
	QueryRow(context.Context, string, ...any) Row
}, projectID, workflowID, revisionID string) (definition.DraftRevision, error) {
	return scanDraftRevision(queryer.QueryRow(ctx, draftRevisionSelect+`
		WHERE project_id = UUID_TO_BIN($1) AND workflow_id = UUID_TO_BIN($2) AND draft_revision_id = UUID_TO_BIN($3)`, projectID, workflowID, revisionID))
}

func insertDraftRevision(ctx context.Context, tx *Tx, record definition.SaveDraftRecord, revision int64, clonedFrom *string) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO workflow_draft_revisions (
			draft_revision_id, project_id, workflow_id, revision_number,
			ir_json, catalog_revision, cloned_from_version_id, created_by
		) VALUES (UUID_TO_BIN($1),UUID_TO_BIN($2),UUID_TO_BIN($3),$4,$5,$6,UUID_TO_BIN($7),UUID_TO_BIN($8))`,
		record.DraftRevisionID, record.ProjectID, record.WorkflowID, revision,
		record.IRJSON, record.CatalogRevision, clonedFrom, record.PrincipalID)
	return err
}

func upsertSnapshot(ctx context.Context, tx *Tx, snapshot definition.ExecutionSnapshot) (definition.ExecutionSnapshot, error) {
	_, err := tx.Exec(ctx, `
		INSERT INTO workflow_execution_snapshots (
			snapshot_id, project_id, workflow_id, origin_kind, origin_id,
			ir_json, dsl_json, source_map_json, compiler_manifest_json,
			ir_hash, dsl_hash, source_map_hash, definition_hash
		) VALUES (UUID_TO_BIN($1),UUID_TO_BIN($2),UUID_TO_BIN($3),$4,UUID_TO_BIN($5),$6,$7,$8,$9,$10,$11,$12,$13)`,
		snapshot.ID, snapshot.ProjectID, snapshot.WorkflowID, snapshot.OriginKind, snapshot.OriginID,
		snapshot.IRJSON, snapshot.DSLJSON, snapshot.SourceMapJSON, snapshot.ManifestJSON,
		snapshot.IRHash, snapshot.DSLHash, snapshot.SourceMapHash, snapshot.DefinitionHash)
	if err != nil && !isDuplicateKey(err) {
		return definition.ExecutionSnapshot{}, err
	}
	stored, err := scanSnapshot(tx.QueryRow(ctx, snapshotSelect+`
		WHERE project_id = UUID_TO_BIN($1) AND workflow_id = UUID_TO_BIN($2) AND definition_hash = $3`,
		snapshot.ProjectID, snapshot.WorkflowID, snapshot.DefinitionHash))
	if errors.Is(err, definition.ErrResourceNotFound) {
		return definition.ExecutionSnapshot{}, fmt.Errorf("definition hash collision or compiler manifest mismatch")
	}
	if err == nil && (stored.IRHash != snapshot.IRHash || stored.DSLHash != snapshot.DSLHash ||
		stored.SourceMapHash != snapshot.SourceMapHash || !jsonValuesEqual(stored.ManifestJSON, snapshot.ManifestJSON)) {
		return definition.ExecutionSnapshot{}, fmt.Errorf("definition hash collision or compiler manifest mismatch")
	}
	return stored, err
}

func jsonValuesEqual(left, right json.RawMessage) bool {
	var leftValue, rightValue any
	if json.Unmarshal(left, &leftValue) != nil || json.Unmarshal(right, &rightValue) != nil {
		return false
	}
	return reflect.DeepEqual(leftValue, rightValue)
}

const snapshotSelect = `
	SELECT BIN_TO_UUID(snapshot_id), BIN_TO_UUID(project_id), BIN_TO_UUID(workflow_id), origin_kind,
	       BIN_TO_UUID(origin_id), ir_json, dsl_json, source_map_json, compiler_manifest_json,
	       ir_hash, dsl_hash, source_map_hash, definition_hash, created_at
	FROM workflow_execution_snapshots `

func scanSnapshot(row scanner) (definition.ExecutionSnapshot, error) {
	var result definition.ExecutionSnapshot
	err := row.Scan(&result.ID, &result.ProjectID, &result.WorkflowID, &result.OriginKind,
		&result.OriginID, &result.IRJSON, &result.DSLJSON, &result.SourceMapJSON, &result.ManifestJSON,
		&result.IRHash, &result.DSLHash, &result.SourceMapHash, &result.DefinitionHash, &result.CreatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return definition.ExecutionSnapshot{}, definition.ErrResourceNotFound
	}
	return result, err
}

const versionSelect = `
	SELECT BIN_TO_UUID(version_id), BIN_TO_UUID(project_id), BIN_TO_UUID(workflow_id), version_number,
	       BIN_TO_UUID(source_draft_revision_id), BIN_TO_UUID(execution_snapshot_id),
	       change_log, BIN_TO_UUID(created_by), created_at
	FROM workflow_versions `

func scanVersion(row scanner) (definition.PublishedVersion, error) {
	var result definition.PublishedVersion
	err := row.Scan(&result.ID, &result.ProjectID, &result.WorkflowID, &result.VersionNumber,
		&result.SourceDraftRevisionID, &result.ExecutionSnapshotID, &result.ChangeLog,
		&result.CreatedBy, &result.CreatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return definition.PublishedVersion{}, definition.ErrResourceNotFound
	}
	return result, err
}

func getVersionAndSnapshotByID(ctx context.Context, queryer interface {
	QueryRow(context.Context, string, ...any) Row
}, projectID, workflowID, versionID string) (definition.PublishedVersion, definition.ExecutionSnapshot, error) {
	version, err := scanVersion(queryer.QueryRow(ctx, versionSelect+`
		WHERE project_id = UUID_TO_BIN($1) AND workflow_id = UUID_TO_BIN($2) AND version_id = UUID_TO_BIN($3)`, projectID, workflowID, versionID))
	if err != nil {
		return definition.PublishedVersion{}, definition.ExecutionSnapshot{}, err
	}
	snapshot, err := scanSnapshot(queryer.QueryRow(ctx, snapshotSelect+`
		WHERE project_id = UUID_TO_BIN($1) AND workflow_id = UUID_TO_BIN($2) AND snapshot_id = UUID_TO_BIN($3)`, projectID, workflowID, version.ExecutionSnapshotID))
	return version, snapshot, err
}

func claimIdempotency(ctx context.Context, tx *Tx, projectID, commandName, targetScope, key,
	requestHash, responseKind, responseID string, auxiliaryID *string,
) (idempotencyResult, bool, error) {
	_, err := tx.Exec(ctx, `
		INSERT INTO definition_idempotency (
			project_id, command_name, target_scope, idempotency_key,
			request_hash, response_kind, response_id, response_aux_id
		) VALUES (UUID_TO_BIN($1),$2,$3,$4,$5,$6,UUID_TO_BIN($7),UUID_TO_BIN($8))`,
		projectID, commandName, targetScope, key, requestHash, responseKind, responseID, auxiliaryID)
	if err == nil {
		return idempotencyResult{}, false, nil
	}
	if !isDuplicateKey(err) {
		return idempotencyResult{}, false, err
	}
	var result idempotencyResult
	err = tx.QueryRow(ctx, `
		SELECT request_hash, BIN_TO_UUID(response_id), BIN_TO_UUID(response_aux_id)
		FROM definition_idempotency
		WHERE project_id = UUID_TO_BIN($1) AND command_name = $2 AND target_scope = $3 AND idempotency_key = $4`,
		projectID, commandName, targetScope, key).Scan(&result.requestHash, &result.responseID, &result.auxiliaryID)
	if errors.Is(err, sql.ErrNoRows) {
		return idempotencyResult{}, false, fmt.Errorf("duplicate idempotency key disappeared")
	}
	return result, err == nil, err
}
