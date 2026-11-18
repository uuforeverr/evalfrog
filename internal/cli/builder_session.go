package cli

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/uu999/evalfrog/internal/ir"
)

const builderSessionVersion = 1

var errBuilderSessionNotFound = errors.New("builder session not found")

type builderSessionMeta struct {
	SessionVersion int    `json:"session_version"`
	Server         string `json:"server,omitempty"`
	ProjectID      string `json:"project_id,omitempty"`
	WorkflowID     string `json:"workflow_id,omitempty"`
	Revision       int64  `json:"revision"`
	BaseIRHash     string `json:"base_ir_hash"`
	CurrentIRHash  string `json:"current_ir_hash"`
}

type builderSession struct {
	Directory string
	Meta      builderSessionMeta
	Document  ir.Document
}

func newBuilderSession(directory string, document ir.Document) (builderSession, error) {
	resolved, err := filepath.Abs(directory)
	if err != nil {
		return builderSession{}, err
	}
	canonical, hash, err := ir.CanonicalDocumentHash(document)
	if err != nil {
		return builderSession{}, err
	}
	parsed, diagnostics := ir.DefaultParser().ParseDraft(canonical)
	if ir.HasErrors(diagnostics) {
		encoded, _ := json.Marshal(diagnostics)
		return builderSession{}, fmt.Errorf("initial IR is invalid: %s", encoded)
	}
	return builderSession{
		Directory: resolved,
		Meta: builderSessionMeta{
			SessionVersion: builderSessionVersion,
			BaseIRHash:     hash,
			CurrentIRHash:  hash,
		},
		Document: parsed,
	}, nil
}

func newEmptyBuilderSession(directory string) (builderSession, error) {
	return newBuilderSession(directory, ir.Document{
		IRVersion: ir.VersionV1,
		Nodes:     []ir.Node{},
		Edges:     []ir.Edge{},
		Layout:    map[ir.LogicalID]ir.Position{},
	})
}

func builderIRPath(directory string) string {
	return filepath.Join(directory, "ir.json")
}

func builderMetaPath(directory string) string {
	return filepath.Join(directory, "meta.json")
}

func loadBuilderSession(directory string) (builderSession, error) {
	resolved, err := filepath.Abs(directory)
	if err != nil {
		return builderSession{}, err
	}
	irRaw, err := os.ReadFile(builderIRPath(resolved))
	if err != nil {
		if os.IsNotExist(err) {
			return builderSession{}, errBuilderSessionNotFound
		}
		return builderSession{}, err
	}
	metaRaw, err := os.ReadFile(builderMetaPath(resolved))
	if err != nil {
		if os.IsNotExist(err) {
			return builderSession{}, errBuilderSessionNotFound
		}
		return builderSession{}, err
	}
	var meta builderSessionMeta
	if err = json.Unmarshal(metaRaw, &meta); err != nil {
		return builderSession{}, fmt.Errorf("decode builder metadata: %w", err)
	}
	if meta.SessionVersion != builderSessionVersion || meta.Revision < 0 || meta.CurrentIRHash == "" || meta.BaseIRHash == "" {
		return builderSession{}, errors.New("builder metadata is invalid")
	}
	document, diagnostics := ir.DefaultParser().ParseDraft(irRaw)
	if ir.HasErrors(diagnostics) {
		encoded, _ := json.Marshal(diagnostics)
		return builderSession{}, fmt.Errorf("builder IR is invalid: %s", encoded)
	}
	_, hash, err := ir.CanonicalDocumentHash(document)
	if err != nil {
		return builderSession{}, err
	}
	if hash != meta.CurrentIRHash {
		return builderSession{}, errors.New("builder IR does not match metadata hash")
	}
	if meta.WorkflowID == "" && (meta.Server != "" || meta.ProjectID != "" || meta.Revision != 0) {
		return builderSession{}, errors.New("builder metadata has an incomplete remote binding")
	}
	if meta.WorkflowID != "" && (meta.Server == "" || meta.ProjectID == "" || meta.Revision < 1) {
		return builderSession{}, errors.New("builder metadata has an invalid remote binding")
	}
	return builderSession{Directory: resolved, Meta: meta, Document: document}, nil
}

func initializeBuilderSession(session *builderSession) error {
	entries, err := os.ReadDir(session.Directory)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	if len(entries) > 0 {
		return errors.New("builder session directory is not empty")
	}
	return saveBuilderSession(session)
}

func saveBuilderSession(session *builderSession) error {
	if session == nil || session.Directory == "" || session.Meta.SessionVersion != builderSessionVersion {
		return errors.New("builder session identity is invalid")
	}
	canonical, hash, err := ir.CanonicalDocumentHash(session.Document)
	if err != nil {
		return err
	}
	parsed, diagnostics := ir.DefaultParser().ParseDraft(canonical)
	if ir.HasErrors(diagnostics) {
		encoded, _ := json.Marshal(diagnostics)
		return fmt.Errorf("builder IR is invalid: %s", encoded)
	}
	session.Document = parsed
	session.Meta.CurrentIRHash = hash
	if session.Meta.BaseIRHash == "" {
		session.Meta.BaseIRHash = hash
	}
	if session.Meta.WorkflowID == "" && (session.Meta.Server != "" || session.Meta.ProjectID != "" || session.Meta.Revision != 0) {
		return errors.New("builder session remote binding is invalid")
	}
	if session.Meta.WorkflowID != "" && (session.Meta.Server == "" || session.Meta.ProjectID == "" || session.Meta.Revision < 1) {
		return errors.New("builder session remote binding is incomplete")
	}
	if err = os.MkdirAll(session.Directory, 0o700); err != nil {
		return err
	}
	if err = writePrivateFile(builderIRPath(session.Directory), append(canonical, '\n')); err != nil {
		return err
	}
	meta, err := json.MarshalIndent(session.Meta, "", "  ")
	if err != nil {
		return err
	}
	return writePrivateFile(builderMetaPath(session.Directory), append(meta, '\n'))
}

func writePrivateFile(path string, content []byte) error {
	temporary := path + ".tmp"
	if err := os.WriteFile(temporary, content, 0o600); err != nil {
		return err
	}
	return os.Rename(temporary, path)
}

func (session builderSession) dirty() bool {
	return session.Meta.CurrentIRHash != session.Meta.BaseIRHash
}

func (session *builderSession) markSynchronized(server, projectID, workflowID string, revision int64, document ir.Document) error {
	canonical, hash, err := ir.CanonicalDocumentHash(document)
	if err != nil {
		return err
	}
	parsed, diagnostics := ir.DefaultParser().ParseDraft(canonical)
	if ir.HasErrors(diagnostics) {
		encoded, _ := json.Marshal(diagnostics)
		return fmt.Errorf("server returned invalid IR: %s", encoded)
	}
	session.Meta.Server = server
	session.Meta.ProjectID = projectID
	session.Meta.WorkflowID = workflowID
	session.Meta.Revision = revision
	session.Meta.BaseIRHash = hash
	session.Meta.CurrentIRHash = hash
	session.Document = parsed
	return nil
}
