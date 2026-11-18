package ir

import (
	"encoding/json"
	"fmt"
	"strings"
	"unicode/utf8"
)

const (
	MaxNodesPerDocument = 10_000
	MaxEdgesPerDocument = 10_000
	MaxPortsPerNode     = 128
	MaxNodeTitleBytes   = 256
)

// NodeContractValidator is the port implemented by the versioned Node Catalog.
// The IR package therefore does not depend on a concrete Catalog implementation.
type NodeContractValidator interface {
	ValidateNode(Node) []Diagnostic
}

type Validator struct {
	nodeContracts NodeContractValidator
}

func NewStrictValidator(nodeContracts NodeContractValidator) Validator {
	if nodeContracts == nil {
		panic("strict IR validator requires a node catalog")
	}
	return Validator{nodeContracts: nodeContracts}
}

func NewStructuralValidator() Validator {
	return Validator{}
}

func (v Validator) Validate(document Document) []Diagnostic {
	diagnostics := make([]Diagnostic, 0)
	if document.IRVersion != VersionV1 && document.IRVersion != VersionV2 {
		diagnostics = append(diagnostics, ErrorDiagnostic(PhaseStructure, "IR_VERSION_UNSUPPORTED", "ir_version must be 1 or 2", Location{IRPath: "/ir_version"}))
	}
	if len(document.Nodes) == 0 {
		diagnostics = append(diagnostics, ErrorDiagnostic(PhaseStructure, "IR_NODES_REQUIRED", "strict validation requires at least one node", Location{IRPath: "/nodes"}))
	}
	if len(document.Nodes) > MaxNodesPerDocument {
		diagnostics = append(diagnostics, ErrorDiagnostic(PhaseStructure, "NODE_LIMIT_EXCEEDED", "IR contains too many nodes", Location{IRPath: "/nodes"}))
	}
	if len(document.Edges) > MaxEdgesPerDocument {
		diagnostics = append(diagnostics, ErrorDiagnostic(PhaseStructure, "EDGE_LIMIT_EXCEEDED", "IR contains too many edges", Location{IRPath: "/edges"}))
	}

	nodes := make(map[LogicalID]Node, len(document.Nodes))
	nodeIndexes := make(map[LogicalID]int, len(document.Nodes))
	outputs := make(map[LogicalID]map[PortName]DataType, len(document.Nodes))
	for index, node := range document.Nodes {
		location := Location{LogicalNodeID: node.ID, IRPath: NodePath(node.ID, index)}
		if !ValidLogicalID(node.ID) {
			diagnostics = append(diagnostics, ErrorDiagnostic(PhaseStructure, "LOGICAL_NODE_ID_INVALID", "node id must be 1-128 safe ASCII characters", location))
		}
		if _, exists := nodes[node.ID]; exists {
			diagnostics = append(diagnostics, ErrorDiagnostic(PhaseStructure, "NODE_ID_DUPLICATE", "node id must be unique within the IR", location))
		} else {
			nodes[node.ID] = node
			nodeIndexes[node.ID] = index
		}
		if !ValidNodeType(node.Type) {
			diagnostics = append(diagnostics, ErrorDiagnostic(PhaseStructure, "NODE_TYPE_INVALID", "node type is invalid", withField(location, "type")))
		}
		if strings.TrimSpace(node.Title) == "" || !utf8.ValidString(node.Title) || len(node.Title) > MaxNodeTitleBytes {
			diagnostics = append(diagnostics, ErrorDiagnostic(PhaseStructure, "NODE_TITLE_INVALID", "node title must be non-empty valid UTF-8 up to 256 bytes", withField(location, "title")))
		}
		if node.Inputs == nil {
			diagnostics = append(diagnostics, ErrorDiagnostic(PhaseStructure, "NODE_INPUTS_REQUIRED", "node inputs must be an array", withField(location, "inputs")))
		}
		if node.Outputs == nil {
			diagnostics = append(diagnostics, ErrorDiagnostic(PhaseStructure, "NODE_OUTPUTS_REQUIRED", "node outputs must be an array", withField(location, "outputs")))
		}
		if len(node.Inputs) > MaxPortsPerNode {
			diagnostics = append(diagnostics, ErrorDiagnostic(PhaseStructure, "INPUT_LIMIT_EXCEEDED", "node contains too many inputs", withField(location, "inputs")))
		}
		if len(node.Outputs) > MaxPortsPerNode {
			diagnostics = append(diagnostics, ErrorDiagnostic(PhaseStructure, "OUTPUT_LIMIT_EXCEEDED", "node contains too many outputs", withField(location, "outputs")))
		}

		inputNames := make(map[PortName]struct{}, len(node.Inputs))
		for inputIndex, input := range node.Inputs {
			inputLocation := Location{LogicalNodeID: node.ID, IRPath: InputPath(node.ID, index, input.Name, inputIndex)}
			if !ValidPortName(input.Name) {
				diagnostics = append(diagnostics, ErrorDiagnostic(PhaseStructure, "INPUT_NAME_INVALID", "input name must be a safe identifier up to 64 bytes", inputLocation))
			}
			if _, exists := inputNames[input.Name]; exists {
				diagnostics = append(diagnostics, ErrorDiagnostic(PhaseStructure, "INPUT_NAME_DUPLICATE", "input name must be unique within a node", inputLocation))
			}
			inputNames[input.Name] = struct{}{}
			if !input.DataType.Valid() {
				diagnostics = append(diagnostics, ErrorDiagnostic(PhaseStructure, "INPUT_DATA_TYPE_INVALID", "input data_type is unsupported", withField(inputLocation, "data_type")))
			}
			diagnostics = append(diagnostics, validateInputSource(input, inputLocation)...)
			if input.Source == SourceLiteral && input.HasValue() && input.DataType.Valid() {
				diagnostics = append(diagnostics, validateLiteral(input, inputLocation)...)
			}
		}

		outputNames := make(map[PortName]DataType, len(node.Outputs))
		for outputIndex, output := range node.Outputs {
			outputLocation := Location{LogicalNodeID: node.ID, IRPath: OutputPath(node.ID, index, output.Name, outputIndex)}
			if !ValidPortName(output.Name) {
				diagnostics = append(diagnostics, ErrorDiagnostic(PhaseStructure, "OUTPUT_NAME_INVALID", "output name must be a safe identifier up to 64 bytes", outputLocation))
			}
			if _, exists := outputNames[output.Name]; exists {
				diagnostics = append(diagnostics, ErrorDiagnostic(PhaseStructure, "OUTPUT_NAME_DUPLICATE", "output name must be unique within a node", outputLocation))
			}
			if !output.DataType.Valid() {
				diagnostics = append(diagnostics, ErrorDiagnostic(PhaseStructure, "OUTPUT_DATA_TYPE_INVALID", "output data_type is unsupported", withField(outputLocation, "data_type")))
			}
			outputNames[output.Name] = output.DataType
		}
		outputs[node.ID] = outputNames
		if v.nodeContracts != nil {
			diagnostics = append(diagnostics, v.nodeContracts.ValidateNode(node)...)
		}
	}

	edgeIDs := make(map[LogicalID]struct{}, len(document.Edges))
	for index, edge := range document.Edges {
		location := Location{LogicalEdgeID: edge.ID, IRPath: EdgePath(edge.ID, index)}
		if !ValidLogicalID(edge.ID) {
			diagnostics = append(diagnostics, ErrorDiagnostic(PhaseStructure, "LOGICAL_EDGE_ID_INVALID", "edge id must be 1-128 safe ASCII characters", location))
		}
		if _, exists := edgeIDs[edge.ID]; exists {
			diagnostics = append(diagnostics, ErrorDiagnostic(PhaseStructure, "EDGE_ID_DUPLICATE", "edge id must be unique within the IR", location))
		}
		edgeIDs[edge.ID] = struct{}{}
		if !ValidLogicalID(edge.Source) {
			diagnostics = append(diagnostics, ErrorDiagnostic(PhaseStructure, "EDGE_SOURCE_ID_INVALID", "edge source must be a valid logical node id", withField(location, "source")))
		}
		if !ValidLogicalID(edge.Target) {
			diagnostics = append(diagnostics, ErrorDiagnostic(PhaseStructure, "EDGE_TARGET_ID_INVALID", "edge target must be a valid logical node id", withField(location, "target")))
		}
		sourceNode, sourceExists := nodes[edge.Source]
		if !sourceExists {
			diagnostics = append(diagnostics, ErrorDiagnostic(PhaseReference, "EDGE_SOURCE_NOT_FOUND", "edge source node does not exist", withField(location, "source")))
		}
		if _, exists := nodes[edge.Target]; !exists {
			diagnostics = append(diagnostics, ErrorDiagnostic(PhaseReference, "EDGE_TARGET_NOT_FOUND", "edge target node does not exist", withField(location, "target")))
		}
		if sourceExists && sourceNode.Type == "branch" {
			if !ValidRouteName(edge.Route) {
				diagnostics = append(diagnostics, ErrorDiagnostic(PhaseStructure, "BRANCH_EDGE_ROUTE_REQUIRED", "branch edge must contain a valid route", withField(location, "route")))
			}
		} else if edge.Route != "" {
			diagnostics = append(diagnostics, ErrorDiagnostic(PhaseStructure, "EDGE_ROUTE_FORBIDDEN", "route is only allowed on branch edges", withField(location, "route")))
		}
	}

	for id := range document.Layout {
		if !ValidLogicalID(id) {
			diagnostics = append(diagnostics, ErrorDiagnostic(PhaseStructure, "LAYOUT_NODE_ID_INVALID", "layout key must be a valid logical node id", Location{LogicalNodeID: id, IRPath: "/layout/" + pointerToken(string(id))}))
		}
		if _, exists := nodes[id]; !exists {
			diagnostics = append(diagnostics, ErrorDiagnostic(PhaseReference, "LAYOUT_NODE_NOT_FOUND", "layout key does not reference an existing node", Location{LogicalNodeID: id, IRPath: "/layout/" + pointerToken(string(id))}))
		}
		position := document.Layout[id]
		if position.X == nil || position.Y == nil {
			diagnostics = append(diagnostics, ErrorDiagnostic(PhaseStructure, "LAYOUT_POSITION_INVALID", "layout position requires numeric x and y", Location{LogicalNodeID: id, IRPath: "/layout/" + pointerToken(string(id))}))
		}
	}
	for id, index := range nodeIndexes {
		if _, exists := document.Layout[id]; !exists {
			diagnostics = append(diagnostics, ErrorDiagnostic(PhaseReference, "LAYOUT_POSITION_MISSING", "every node requires a shared layout position", Location{LogicalNodeID: id, IRPath: NodePath(id, index)}))
		}
	}

	for nodeIndex, node := range document.Nodes {
		for inputIndex, input := range node.Inputs {
			if input.Source != SourceRef {
				continue
			}
			location := Location{LogicalNodeID: node.ID, IRPath: InputPath(node.ID, nodeIndex, input.Name, inputIndex)}
			sourceOutputs, nodeExists := outputs[input.RefNode]
			if !nodeExists {
				diagnostics = append(diagnostics, ErrorDiagnostic(PhaseReference, "REF_NODE_NOT_FOUND", "referenced node does not exist", withField(location, "ref_node")))
				continue
			}
			sourceType, outputExists := sourceOutputs[input.RefOutput]
			if !outputExists {
				diagnostics = append(diagnostics, ErrorDiagnostic(PhaseReference, "REF_OUTPUT_NOT_FOUND", "referenced output does not exist", withField(location, "ref_output")))
				continue
			}
			if input.DataType.Valid() && sourceType.Valid() && !Compatible(sourceType, input.DataType) {
				diagnostic := ErrorDiagnostic(PhaseReference, "REF_TYPE_MISMATCH", "referenced output type is incompatible with input type", withField(location, "data_type"))
				diagnostic.Details = map[string]any{"source": sourceType, "target": input.DataType}
				diagnostics = append(diagnostics, diagnostic)
			}
		}
	}

	SortDiagnostics(diagnostics)
	return LimitDiagnostics(diagnostics)
}

func validateInputSource(input Input, location Location) []Diagnostic {
	if !input.Source.Valid() {
		return []Diagnostic{ErrorDiagnostic(PhaseStructure, "INPUT_SOURCE_INVALID", "input source must be literal or ref", withField(location, "source"))}
	}
	valid := false
	switch input.Source {
	case SourceLiteral:
		valid = input.HasValue() && input.RefNode == "" && input.RefOutput == ""
	case SourceRef:
		valid = !input.HasValue() && input.RefNode != "" && input.RefOutput != ""
	}
	if valid {
		if input.Source == SourceRef && (!ValidLogicalID(input.RefNode) || !ValidPortName(input.RefOutput)) {
			return []Diagnostic{ErrorDiagnostic(PhaseStructure, "INPUT_REF_ID_INVALID", "ref_node and ref_output must be valid author identifiers", location)}
		}
		return nil
	}
	return []Diagnostic{ErrorDiagnostic(PhaseStructure, "INPUT_SOURCE_FIELDS_INVALID", "literal and ref fields are mutually exclusive", location)}
}

func validateLiteral(input Input, location Location) []Diagnostic {
	value, actual, err := DecodeLiteral(input.Value)
	if err != nil {
		return []Diagnostic{ErrorDiagnostic(PhaseStructure, "LITERAL_INVALID", err.Error(), withField(location, "value"))}
	}
	if input.DataType == TypeInteger {
		number, ok := value.(json.Number)
		if ok && actual == TypeInteger && !SafeInteger(number) {
			return []Diagnostic{ErrorDiagnostic(PhaseStructure, "INTEGER_OUT_OF_RANGE", "integer exceeds the JavaScript safe integer range", withField(location, "value"))}
		}
	}
	if !Compatible(actual, input.DataType) {
		diagnostic := ErrorDiagnostic(PhaseStructure, "LITERAL_TYPE_MISMATCH", "literal value does not match data_type", withField(location, "value"))
		diagnostic.Details = map[string]any{"expected": input.DataType, "actual": actual}
		return []Diagnostic{diagnostic}
	}
	return nil
}

func withField(location Location, field string) Location {
	copy := location
	copy.IRPath = strings.TrimSuffix(copy.IRPath, "/") + "/" + field
	return copy
}

func DiagnosticsError(values []Diagnostic) error {
	if !HasErrors(values) {
		return nil
	}
	return fmt.Errorf("IR validation failed with %d diagnostic(s)", len(values))
}
