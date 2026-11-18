// Package ir owns EvalFrog's author-facing workflow representation.
package ir

import (
	"encoding/json"
)

const VersionV1 = "1"
const VersionV2 = "2"

type LogicalID string
type NodeType string
type PortName string
type RouteName string

type Document struct {
	IRVersion string                 `json:"ir_version"`
	Nodes     []Node                 `json:"nodes"`
	Edges     []Edge                 `json:"edges"`
	Layout    map[LogicalID]Position `json:"layout"`
}

type Node struct {
	ID      LogicalID `json:"id"`
	Type    NodeType  `json:"type"`
	Title   string    `json:"title"`
	Inputs  []Input   `json:"inputs"`
	Outputs []Output  `json:"outputs"`
}

type Input struct {
	Name      PortName        `json:"name"`
	DataType  DataType        `json:"data_type"`
	Source    InputSource     `json:"source"`
	Value     json.RawMessage `json:"value,omitempty"`
	RefNode   LogicalID       `json:"ref_node,omitempty"`
	RefOutput PortName        `json:"ref_output,omitempty"`
}

func (i Input) HasValue() bool {
	return i.Value != nil
}

type Output struct {
	Name     PortName `json:"name"`
	DataType DataType `json:"data_type"`
}

type Edge struct {
	ID     LogicalID `json:"id"`
	Source LogicalID `json:"source"`
	Target LogicalID `json:"target"`
	Route  RouteName `json:"route,omitempty"`
}

type Position struct {
	X *json.Number `json:"x"`
	Y *json.Number `json:"y"`
}

type InputSource string

const (
	SourceLiteral InputSource = "literal"
	SourceRef     InputSource = "ref"
)

func (s InputSource) Valid() bool {
	return s == SourceLiteral || s == SourceRef
}
