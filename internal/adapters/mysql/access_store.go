package mysql

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"

	"github.com/uu999/evalfrog/internal/access"
)

func (store *Store) FindPrincipalByCredentialHash(ctx context.Context, digest [sha256.Size]byte) (access.Principal, error) {
	var result access.Principal
	err := store.pool.QueryRow(ctx, `
		SELECT BIN_TO_UUID(p.principal_id), p.kind, p.display_name
		FROM principal_credentials c
		JOIN principals p ON p.principal_id = c.principal_id
		WHERE c.credential_hash = $1
		  AND c.revoked_at IS NULL
		  AND (c.expires_at IS NULL OR c.expires_at > CURRENT_TIMESTAMP(6))
		  AND p.enabled`, digest[:]).Scan(&result.ID, &result.Kind, &result.Name)
	if errors.Is(err, sql.ErrNoRows) {
		return access.Principal{}, access.ErrResourceNotFound
	}
	return result, err
}

func (store *Store) HasPermission(ctx context.Context, projectID, principalID string, permission access.Permission) (bool, error) {
	var allowed bool
	err := store.pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM project_membership_permissions p
			JOIN principals principal ON principal.principal_id = p.principal_id AND principal.enabled
			WHERE p.project_id = UUID_TO_BIN($1) AND p.principal_id = UUID_TO_BIN($2) AND p.permission = $3
		)`, projectID, principalID, permission).Scan(&allowed)
	return allowed, err
}

func (store *Store) FindExecutionIdentity(ctx context.Context, projectID string) (access.ExecutionIdentity, error) {
	var result access.ExecutionIdentity
	err := store.pool.QueryRow(ctx, `
		SELECT BIN_TO_UUID(execution_identity_id), BIN_TO_UUID(project_id)
		FROM project_execution_identities
		WHERE project_id = UUID_TO_BIN($1) AND enabled`, projectID).Scan(&result.ID, &result.ProjectID)
	if errors.Is(err, sql.ErrNoRows) {
		return access.ExecutionIdentity{}, access.ErrResourceNotFound
	}
	return result, err
}
