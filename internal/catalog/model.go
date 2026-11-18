// Package catalog owns versioned node authoring contracts and public descriptions.
package catalog

import (
	"github.com/uu999/evalfrog/internal/ir"
)

type RevisionID string

const BuiltinRevisionV1 RevisionID = "catalog-v1"
const BuiltinRevisionV2 RevisionID = "catalog-v2"

type NodeKind string

const (
	KindControl NodeKind = "control"
	KindTask    NodeKind = "task"
)

type OutputMode string

const (
	OutputFixed          OutputMode = "fixed"
	OutputAuthorDeclared OutputMode = "author_declared"
)

type NodeDescription struct {
	Type             ir.NodeType                 `json:"type"`
	Kind             NodeKind                    `json:"kind"`
	Description      string                      `json:"natural_language_description"`
	Inputs           []InputDescription          `json:"allowed_inputs"`
	AdditionalInputs *AdditionalInputDescription `json:"additional_inputs,omitempty"`
	Outputs          OutputDescription           `json:"outputs"`
	Examples         []ir.Node                   `json:"examples"`
}

type InputDescription struct {
	Name        ir.PortName         `json:"name"`
	Description string              `json:"description"`
	Required    bool                `json:"required"`
	DataTypes   []ir.DataType       `json:"data_types"`
	Sources     []ir.InputSource    `json:"sources"`
	Constraints *LiteralConstraints `json:"literal_constraints,omitempty"`
}

type AdditionalInputDescription struct {
	Description   string           `json:"description"`
	DataTypes     []ir.DataType    `json:"data_types"`
	Sources       []ir.InputSource `json:"sources"`
	ReservedNames []ir.PortName    `json:"reserved_names,omitempty"`
}

type LiteralConstraints struct {
	StringEnum    []string               `json:"string_enum,omitempty"`
	StringPattern string                 `json:"string_pattern,omitempty"`
	MinLength     int                    `json:"min_length,omitempty"`
	MaxLength     int                    `json:"max_length,omitempty"`
	ArrayItems    *ArrayItemConstraint   `json:"array_items,omitempty"`
	BranchCases   *BranchCaseConstraints `json:"branch_cases,omitempty"`
}

type ArrayItemConstraint struct {
	DataType ir.DataType `json:"data_type"`
	Minimum  *int64      `json:"minimum,omitempty"`
	Maximum  *int64      `json:"maximum,omitempty"`
}

type BranchCaseConstraints struct {
	FirstMatchWins bool                     `json:"first_match_wins"`
	PathSyntax     string                   `json:"path_syntax"`
	Operators      map[ir.DataType][]string `json:"operators"`
}

type OutputDescription struct {
	Mode             OutputMode        `json:"mode"`
	Fields           []PortDescription `json:"fields,omitempty"`
	AllowedDataTypes []ir.DataType     `json:"allowed_data_types,omitempty"`
	MinFields        int               `json:"min_fields,omitempty"`
	MaxFields        int               `json:"max_fields,omitempty"`
}

type PortDescription struct {
	Name        ir.PortName `json:"name"`
	Description string      `json:"description"`
	DataType    ir.DataType `json:"data_type"`
}

// RuntimeContract is internal compiler metadata. It is intentionally absent
// from NodeDescription so authors never select operation or contract versions.
type RuntimeContract struct {
	Kind                   NodeKind
	OperationType          string
	OperationVersion       uint32
	DefaultExecutionPolicy ExecutionPolicyDefaults
}

type ExecutionPolicyDefaults struct {
	MaxAttempts         uint32
	MaxRecoveries       uint32
	AttemptTimeoutMS    uint64
	RetryBackoffMS      uint64
	RetryableErrorCodes []string
}
