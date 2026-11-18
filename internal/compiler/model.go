// Package compiler transforms author-facing IR into immutable runtime DSL and
// Source Map artifacts. It is a deterministic domain module and has no storage,
// broker, cache, or transport dependencies.
package compiler

import (
	"encoding/json"

	"github.com/uu999/evalfrog/internal/catalog"
	"github.com/uu999/evalfrog/internal/dsl"
	"github.com/uu999/evalfrog/internal/ir"
	"github.com/uu999/evalfrog/internal/sourcemap"
)

const VersionV1 = "compiler-v1"
const VersionV2 = "compiler-v2"

type NodeCatalog interface {
	Revision() catalog.RevisionID
	ValidateNode(ir.Node) []ir.Diagnostic
	RuntimeContract(ir.NodeType) (catalog.RuntimeContract, bool)
}

type Request struct {
	IR        ir.Document
	Catalog   NodeCatalog
	Policy    Policy
	Resources ResourceBindings
}

type Manifest struct {
	CompilerVersion string `json:"compiler_version"`
	CatalogRevision string `json:"catalog_revision"`
	PolicyRevision  string `json:"policy_revision"`
}

type Hashes struct {
	IRHash         string `json:"ir_hash"`
	DSLHash        string `json:"dsl_hash"`
	SourceMapHash  string `json:"source_map_hash"`
	DefinitionHash string `json:"definition_hash"`
}

type Result struct {
	DSL                dsl.Document
	SourceMap          sourcemap.Document
	Manifest           Manifest
	Hashes             Hashes
	CanonicalIR        []byte
	CanonicalDSL       []byte
	CanonicalSourceMap []byte
}

type NodeProduct struct {
	Config        map[string]json.RawMessage
	Inputs        map[dsl.PortName]dsl.InputBinding
	Outputs       map[dsl.PortName]dsl.DataType
	FieldMappings map[string]string
}

type Handler interface {
	NodeType() ir.NodeType
	Kind() dsl.NodeKind
	Coordinate() dsl.Coordinate
	Compile(Context, ir.Node) (NodeProduct, []ir.Diagnostic)
}

type Context struct {
	executionNodeID dsl.NodeID
	executionIDs    map[ir.LogicalID]dsl.NodeID
	policy          dsl.ExecutionPolicy
	resources       ResourceBindings
	nodeIndex       int
}

func (context Context) ExecutionNodeID() dsl.NodeID {
	return context.executionNodeID
}

func (context Context) ExecutionNodeIDFor(logicalID ir.LogicalID) (dsl.NodeID, bool) {
	value, exists := context.executionIDs[logicalID]
	return value, exists
}

func (context Context) ExecutionPolicy() dsl.ExecutionPolicy {
	return clonePolicy(context.policy)
}

func (context Context) Resources() ResourceBindings {
	return context.resources
}

func (context Context) NodeIndex() int {
	return context.nodeIndex
}
