package catalog

import (
	"fmt"
	"github.com/uu999/evalfrog/internal/ir"
	"strings"
)

func branchContractV2() contract {
	return contract{
		revision: 2,
		runtime:  RuntimeContract{Kind: KindControl, OperationType: "control.branch", OperationVersion: 2},
		description: NodeDescription{
			Type: "branch", Kind: KindControl,
			Description: "Selects the first matching IF or ELSE IF group; otherwise selects ELSE.",
			Inputs: []InputDescription{
				{Name: "cases", Description: "Ordered condition groups.", Required: true, DataTypes: []ir.DataType{ir.TypeArray}, Sources: literalOnly},
				{Name: "default_route", Description: "ELSE route.", Required: true, DataTypes: []ir.DataType{ir.TypeString}, Sources: literalOnly},
			},
			AdditionalInputs: &AdditionalInputDescription{Description: "Named value used by conditions.", DataTypes: allTypes, Sources: bothSources, ReservedNames: []ir.PortName{"cases", "default_route"}},
			Outputs:          fixedOutputs(),
			Examples: []ir.Node{{ID: "branch", Type: "branch", Title: "条件分支", Inputs: []ir.Input{
				{Name: "score", DataType: ir.TypeInteger, Source: ir.SourceLiteral, Value: mustJSON(0)},
				{Name: "cases", DataType: ir.TypeArray, Source: ir.SourceLiteral, Value: mustJSON([]any{map[string]any{"route": "passed", "logic": "and", "conditions": []any{map[string]any{"input": "score", "operator": "gte", "value": 60}}}})},
				{Name: "default_route", DataType: ir.TypeString, Source: ir.SourceLiteral, Value: mustJSON("else")},
			}, Outputs: []ir.Output{}}},
		},
		validate: validateBranchV2,
	}
}

func validateBranchV2(node ir.Node) []ir.Diagnostic {
	inputs := make(map[string]ir.DataType)
	for _, input := range node.Inputs {
		if input.Name != "cases" && input.Name != "default_route" {
			inputs[string(input.Name)] = input.DataType
		}
	}
	for index, input := range node.Inputs {
		if input.Name == "default_route" {
			value, _, err := ir.DecodeLiteral(input.Value)
			if err == nil {
				route, ok := value.(string)
				if !ok || !ir.ValidRouteName(ir.RouteName(route)) {
					return []ir.Diagnostic{nodeInputDiagnostic(node, input, index, "BRANCH_ROUTE_INVALID", "ELSE route is invalid")}
				}
			}
		}
		if input.Name != "cases" {
			continue
		}
		value, _, err := ir.DecodeLiteral(input.Value)
		if err != nil {
			continue
		}
		cases, ok := value.([]any)
		if !ok || len(cases) == 0 {
			return []ir.Diagnostic{nodeInputDiagnostic(node, input, index, "BRANCH_CASES_INVALID", "at least one IF condition is required")}
		}
		for caseIndex, raw := range cases {
			item, ok := raw.(map[string]any)
			if !ok {
				return []ir.Diagnostic{branchCaseDiagnostic(node, input, index, caseIndex, "case must be an object")}
			}
			route, routeOK := item["route"].(string)
			logic, logicOK := item["logic"].(string)
			conditions, conditionsOK := item["conditions"].([]any)
			if !routeOK || !ir.ValidRouteName(ir.RouteName(route)) || !logicOK || logic != "and" && logic != "or" || !conditionsOK || len(conditions) == 0 {
				return []ir.Diagnostic{branchCaseDiagnostic(node, input, index, caseIndex, "case requires route, and/or logic, and conditions")}
			}
			for key := range item {
				if key != "route" && key != "logic" && key != "conditions" {
					return []ir.Diagnostic{branchCaseDiagnostic(node, input, index, caseIndex, fmt.Sprintf("unsupported field %s", key))}
				}
			}
			for conditionIndex, rawCondition := range conditions {
				condition, ok := rawCondition.(map[string]any)
				if !ok {
					return []ir.Diagnostic{branchCaseDiagnostic(node, input, index, caseIndex, "condition must be an object")}
				}
				name, nameOK := condition["input"].(string)
				operator, operatorOK := condition["operator"].(string)
				typeOfInput, exists := inputs[name]
				comparison, hasValue := condition["value"]
				if !nameOK || !exists || !operatorOK || !hasValue {
					return []ir.Diagnostic{branchCaseDiagnostic(node, input, index, caseIndex, fmt.Sprintf("condition %d requires a declared input, operator, and value", conditionIndex))}
				}
				path, hasPath := condition["path"]
				if hasPath {
					pathText, ok := path.(string)
					if !ok || !validBranchPath(pathText) || typeOfInput != ir.TypeObject || !validPathComparison(operator, comparison) {
						return []ir.Diagnostic{branchCaseTypeDiagnostic(node, input, index, caseIndex, "condition path or comparison is incompatible")}
					}
				} else if !containsString(branchOperatorMatrix[typeOfInput], operator) || !validBranchComparison(typeOfInput, operator, comparison) {
					return []ir.Diagnostic{branchCaseTypeDiagnostic(node, input, index, caseIndex, "condition operator or comparison is incompatible")}
				}
				for key := range condition {
					if !strings.Contains("|input|path|operator|value|", "|"+key+"|") {
						return []ir.Diagnostic{branchCaseDiagnostic(node, input, index, caseIndex, fmt.Sprintf("unsupported condition field %s", key))}
					}
				}
			}
		}
	}
	return nil
}

// The v2 control contracts expose workflow parameters as named ports. Their
// operation coordinates differ from v1 so old snapshots retain their meaning.
func startContractV2() contract {
	return contract{
		revision: 2,
		runtime:  RuntimeContract{Kind: KindControl, OperationType: "control.start", OperationVersion: 2},
		description: NodeDescription{
			Type: "start", Kind: KindControl,
			Description: "Declares the names and JSON types accepted at workflow invocation.",
			Inputs:      []InputDescription{},
			Outputs:     OutputDescription{Mode: OutputAuthorDeclared, AllowedDataTypes: allTypes, MinFields: 0, MaxFields: ir.MaxPortsPerNode},
			Examples:    []ir.Node{{ID: "start", Type: "start", Title: "开始", Inputs: []ir.Input{}, Outputs: []ir.Output{{Name: "request", DataType: ir.TypeObject}}}},
		},
	}
}

func endContractV2() contract {
	return contract{
		revision: 2,
		runtime:  RuntimeContract{Kind: KindControl, OperationType: "control.end", OperationVersion: 2},
		description: NodeDescription{
			Type: "end", Kind: KindControl,
			Description:      "Returns named workflow outputs from literal values or upstream node outputs.",
			Inputs:           []InputDescription{},
			AdditionalInputs: &AdditionalInputDescription{Description: "Value returned under this name.", DataTypes: allTypes, Sources: bothSources},
			Outputs:          OutputDescription{Mode: OutputAuthorDeclared, AllowedDataTypes: allTypes, MinFields: 0, MaxFields: ir.MaxPortsPerNode},
			Examples:         []ir.Node{{ID: "end", Type: "end", Title: "结束", Inputs: []ir.Input{{Name: "result", DataType: ir.TypeObject, Source: ir.SourceLiteral, Value: mustJSON(map[string]any{})}}, Outputs: []ir.Output{{Name: "result", DataType: ir.TypeObject}}}},
		},
		validate: validateEndV2,
	}
}

func validateEndV2(node ir.Node) []ir.Diagnostic {
	inputs := make(map[ir.PortName]ir.DataType, len(node.Inputs))
	for _, input := range node.Inputs {
		inputs[input.Name] = input.DataType
	}
	outputs := make(map[ir.PortName]ir.DataType, len(node.Outputs))
	for _, output := range node.Outputs {
		outputs[output.Name] = output.DataType
	}
	if len(inputs) != len(outputs) {
		return []ir.Diagnostic{ir.ErrorDiagnostic(ir.PhaseCatalog, "END_OUTPUT_BINDING_MISMATCH", "end outputs must have one same-named input value each", ir.Location{LogicalNodeID: node.ID, IRPath: "/nodes/" + string(node.ID) + "/outputs"})}
	}
	for name, dataType := range outputs {
		if inputs[name] != dataType {
			return []ir.Diagnostic{ir.ErrorDiagnostic(ir.PhaseCatalog, "END_OUTPUT_BINDING_MISMATCH", "end output name and data_type must match its input value", ir.Location{LogicalNodeID: node.ID, IRPath: "/nodes/" + string(node.ID) + "/outputs/" + string(name)})}
		}
	}
	return nil
}
