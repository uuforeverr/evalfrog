package compiler

import (
	"encoding/json"
	"fmt"

	"github.com/uu999/evalfrog/internal/dsl"
	"github.com/uu999/evalfrog/internal/ir"
)

type builtinHandler struct {
	nodeType   ir.NodeType
	kind       dsl.NodeKind
	coordinate dsl.Coordinate
	compile    func(Context, ir.Node) (NodeProduct, []ir.Diagnostic)
}

func (handler builtinHandler) NodeType() ir.NodeType      { return handler.nodeType }
func (handler builtinHandler) Kind() dsl.NodeKind         { return handler.kind }
func (handler builtinHandler) Coordinate() dsl.Coordinate { return handler.coordinate }
func (handler builtinHandler) Compile(context Context, node ir.Node) (NodeProduct, []ir.Diagnostic) {
	return handler.compile(context, node)
}

func BuiltinRegistry() Registry {
	registry, err := NewRegistry(
		builtinHandler{nodeType: "start", kind: dsl.KindControl, coordinate: dsl.Coordinate{Type: "control.start", Version: 1}, compile: compileStart},
		builtinHandler{nodeType: "end", kind: dsl.KindControl, coordinate: dsl.Coordinate{Type: "control.end", Version: 1}, compile: compileEnd},
		builtinHandler{nodeType: "branch", kind: dsl.KindControl, coordinate: dsl.Coordinate{Type: "control.branch", Version: 1}, compile: compileBranch},
		builtinHandler{nodeType: "code", kind: dsl.KindTask, coordinate: dsl.Coordinate{Type: "task.python", Version: 1}, compile: compileCode},
		builtinHandler{nodeType: "http", kind: dsl.KindTask, coordinate: dsl.Coordinate{Type: "task.http", Version: 1}, compile: compileHTTP},
		builtinHandler{nodeType: "rpc", kind: dsl.KindTask, coordinate: dsl.Coordinate{Type: "task.rpc", Version: 1}, compile: compileRPC},
	)
	if err != nil {
		panic(err)
	}
	return registry
}

func BuiltinRegistryV2() Registry {
	registry, err := NewRegistry(
		builtinHandler{nodeType: "start", kind: dsl.KindControl, coordinate: dsl.Coordinate{Type: "control.start", Version: 2}, compile: compileStart},
		builtinHandler{nodeType: "end", kind: dsl.KindControl, coordinate: dsl.Coordinate{Type: "control.end", Version: 2}, compile: compileEndV2},
		builtinHandler{nodeType: "branch", kind: dsl.KindControl, coordinate: dsl.Coordinate{Type: "control.branch", Version: 2}, compile: compileBranchV2},
		builtinHandler{nodeType: "code", kind: dsl.KindTask, coordinate: dsl.Coordinate{Type: "task.python", Version: 1}, compile: compileCode},
		builtinHandler{nodeType: "http", kind: dsl.KindTask, coordinate: dsl.Coordinate{Type: "task.http", Version: 1}, compile: compileHTTP},
		builtinHandler{nodeType: "rpc", kind: dsl.KindTask, coordinate: dsl.Coordinate{Type: "task.rpc", Version: 1}, compile: compileRPC},
	)
	if err != nil {
		panic(err)
	}
	return registry
}

func compileBranchV2(context Context, node ir.Node) (NodeProduct, []ir.Diagnostic) {
	product := emptyProduct(node)
	var diagnostics []ir.Diagnostic
	for _, input := range node.Inputs {
		if input.Name == "cases" || input.Name == "default_route" {
			canonical, err := ir.CanonicalizeJSON(input.Value, ir.DefaultParseLimits)
			if err != nil {
				diagnostics = append(diagnostics, inputDiagnostic(context, node, input.Name, "BRANCH_CONFIG_INVALID", err.Error()))
				continue
			}
			product.Config[string(input.Name)] = canonical
			product.FieldMappings["operation.config."+string(input.Name)] = "inputs." + string(input.Name)
			if input.Name == "cases" {
				var cases []map[string]json.RawMessage
				if json.Unmarshal(canonical, &cases) == nil {
					for index, branchCase := range cases {
						for field := range branchCase {
							product.FieldMappings[fmt.Sprintf("operation.config.cases.%d.%s", index, field)] = fmt.Sprintf("inputs.cases.%d.%s", index, field)
						}
						var conditions []map[string]json.RawMessage
						if json.Unmarshal(branchCase["conditions"], &conditions) == nil {
							for conditionIndex, condition := range conditions {
								for field := range condition {
									product.FieldMappings[fmt.Sprintf("operation.config.cases.%d.conditions.%d.%s", index, conditionIndex, field)] = fmt.Sprintf("inputs.cases.%d.conditions.%d.%s", index, conditionIndex, field)
								}
							}
						}
					}
				}
			}
			continue
		}
		binding, issues := compileInput(context, node, input)
		diagnostics = append(diagnostics, issues...)
		if !ir.HasErrors(issues) {
			product.Inputs[dsl.PortName(input.Name)] = binding
			product.FieldMappings["inputs."+string(input.Name)] = "inputs." + string(input.Name)
		}
	}
	return product, diagnostics
}

func compileStart(_ Context, node ir.Node) (NodeProduct, []ir.Diagnostic) {
	return NodeProduct{
		Config: map[string]json.RawMessage{}, Inputs: map[dsl.PortName]dsl.InputBinding{},
		Outputs: compileOutputs(node), FieldMappings: outputMappings(node),
	}, nil
}

func compileEnd(context Context, node ir.Node) (NodeProduct, []ir.Diagnostic) {
	product := emptyProduct(node)
	input, diagnostics := compileNamedInput(context, node, "workflow_output")
	if !ir.HasErrors(diagnostics) {
		product.Inputs["workflow_output"] = input
		product.FieldMappings["inputs.workflow_output"] = "inputs.workflow_output"
	}
	return product, diagnostics
}

func compileEndV2(context Context, node ir.Node) (NodeProduct, []ir.Diagnostic) {
	product := emptyProduct(node)
	var diagnostics []ir.Diagnostic
	for _, input := range node.Inputs {
		binding, issues := compileInput(context, node, input)
		diagnostics = append(diagnostics, issues...)
		if !ir.HasErrors(issues) {
			product.Inputs[dsl.PortName(input.Name)] = binding
			product.FieldMappings["inputs."+string(input.Name)] = "inputs." + string(input.Name)
		}
	}
	return product, diagnostics
}

func compileBranch(context Context, node ir.Node) (NodeProduct, []ir.Diagnostic) {
	product := emptyProduct(node)
	diagnostics := make([]ir.Diagnostic, 0)
	value, valueDiagnostics := compileNamedInput(context, node, "value")
	diagnostics = append(diagnostics, valueDiagnostics...)
	if !ir.HasErrors(valueDiagnostics) {
		product.Inputs["value"] = value
		product.FieldMappings["inputs.value"] = "inputs.value"
	}
	for _, name := range []ir.PortName{"cases", "default_route"} {
		input, _, exists := findInput(node, name)
		if !exists || input.Source != ir.SourceLiteral {
			continue
		}
		canonical, err := ir.CanonicalizeJSON(input.Value, ir.DefaultParseLimits)
		if err != nil {
			diagnostics = append(diagnostics, inputDiagnostic(context, node, name, "BRANCH_CONFIG_INVALID", err.Error()))
			continue
		}
		product.Config[string(name)] = canonical
		product.FieldMappings["operation.config."+string(name)] = "inputs." + string(name)
	}
	var cases []map[string]json.RawMessage
	if raw, exists := product.Config["cases"]; exists && json.Unmarshal(raw, &cases) == nil {
		for index, branchCase := range cases {
			var route string
			_ = json.Unmarshal(branchCase["route"], &route)
			for field := range branchCase {
				product.FieldMappings[fmt.Sprintf("operation.config.cases.%d.%s", index, field)] = fmt.Sprintf("inputs.cases.%s.%s", route, field)
			}
		}
	}
	return product, diagnostics
}

func compileCode(context Context, node ir.Node) (NodeProduct, []ir.Diagnostic) {
	product := emptyProduct(node)
	diagnostics := make([]ir.Diagnostic, 0)
	if context.policy.MaxAttempts > 1 {
		diagnostics = append(diagnostics, inputDiagnostic(context, node, "source_code", "CODE_RETRY_POLICY_UNSUPPORTED", "Code Node business failures are not automatically retryable in DSL v1"))
	}
	sourceCode, _, exists := findInput(node, "source_code")
	if exists && sourceCode.Source == ir.SourceLiteral {
		canonical, err := ir.CanonicalizeJSON(sourceCode.Value, ir.DefaultParseLimits)
		if err != nil {
			diagnostics = append(diagnostics, inputDiagnostic(context, node, "source_code", "CODE_SOURCE_INVALID", err.Error()))
		} else {
			product.Config["source_code"] = canonical
			product.FieldMappings["operation.config.source_code"] = "inputs.source_code"
		}
	}
	product.Config["sandbox_profile"] = mustRaw("python-sandbox-v1")
	if modelInput, _, hasModel := findInput(node, "model_ref"); hasModel {
		modelRef, ok := literalString(modelInput)
		binding, resolved := context.resources.Model(modelRef)
		if !ok || !resolved {
			diagnostics = append(diagnostics, inputPhaseDiagnostic(context, node, "model_ref", ir.PhaseResource, "MODEL_BINDING_REQUIRED", "model_ref has no authorized immutable model binding"))
		} else {
			product.Config["model_artifact"] = mustRaw(map[string]any{"id": binding.ArtifactID, "digest": binding.Digest})
			product.FieldMappings["operation.config.model_artifact"] = "inputs.model_ref"
			product.FieldMappings["operation.config.model_artifact.id"] = "inputs.model_ref"
			product.FieldMappings["operation.config.model_artifact.digest"] = "inputs.model_ref"
		}
	}
	for _, input := range node.Inputs {
		if input.Name == "source_code" || input.Name == "model_ref" {
			continue
		}
		binding, values := compileInput(context, node, input)
		diagnostics = append(diagnostics, values...)
		if !ir.HasErrors(values) {
			product.Inputs[dsl.PortName(input.Name)] = binding
			product.FieldMappings["inputs."+string(input.Name)] = "inputs." + string(input.Name)
		}
	}
	return product, diagnostics
}

func compileHTTP(context Context, node ir.Node) (NodeProduct, []ir.Diagnostic) {
	product := emptyProduct(node)
	diagnostics := make([]ir.Diagnostic, 0)
	connectionInput, _, hasConnection := findInput(node, "connection_ref")
	connectionRef, validConnection := literalString(connectionInput)
	connection, resolvedConnection := context.resources.Connection(connectionRef)
	if !hasConnection || !validConnection || !resolvedConnection {
		diagnostics = append(diagnostics, inputPhaseDiagnostic(context, node, "connection_ref", ir.PhaseResource, "CONNECTION_BINDING_REQUIRED", "connection_ref has no authorized stable connection binding"))
	} else {
		product.Config["connection_id"] = mustRaw(connection.ConnectionID)
		product.FieldMappings["operation.config.connection_id"] = "inputs.connection_ref"
	}
	method, _, hasMethod := findInput(node, "method")
	if hasMethod && method.Source == ir.SourceLiteral {
		product.Config["method"] = append(json.RawMessage(nil), method.Value...)
		product.FieldMappings["operation.config.method"] = "inputs.method"
		methodValue, _ := literalString(method)
		if context.policy.MaxAttempts > 1 && methodValue != "GET" {
			diagnostics = append(diagnostics, inputDiagnostic(context, node, "method", "HTTP_RETRY_POLICY_UNSAFE", "automatic business retry requires an idempotent HTTP operation"))
		}
	}
	statuses, _, hasStatuses := findInput(node, "accepted_statuses")
	if hasStatuses && statuses.Source == ir.SourceLiteral {
		canonical, err := ir.CanonicalizeJSON(statuses.Value, ir.DefaultParseLimits)
		if err != nil {
			diagnostics = append(diagnostics, inputDiagnostic(context, node, "accepted_statuses", "HTTP_STATUSES_INVALID", err.Error()))
		} else {
			product.Config["accepted_statuses"] = canonical
		}
	} else {
		defaults := make([]int, 0, 100)
		for status := 200; status <= 299; status++ {
			defaults = append(defaults, status)
		}
		product.Config["accepted_statuses"] = mustRaw(defaults)
	}
	product.FieldMappings["operation.config.accepted_statuses"] = "inputs.accepted_statuses"
	for _, name := range []ir.PortName{"relative_path", "query", "headers", "body"} {
		input, _, exists := findInput(node, name)
		if !exists {
			continue
		}
		binding, values := compileInput(context, node, input)
		diagnostics = append(diagnostics, values...)
		if !ir.HasErrors(values) {
			product.Inputs[dsl.PortName(name)] = binding
			product.FieldMappings["inputs."+string(name)] = "inputs." + string(name)
		}
	}
	return product, diagnostics
}

func compileRPC(context Context, node ir.Node) (NodeProduct, []ir.Diagnostic) {
	product := emptyProduct(node)
	diagnostics := make([]ir.Diagnostic, 0)
	serviceInput, _, hasService := findInput(node, "service_ref")
	operationInput, _, hasOperation := findInput(node, "operation")
	serviceRef, validService := literalString(serviceInput)
	operation, validOperation := literalString(operationInput)
	resolved, exists := context.resources.ServiceOperation(serviceRef, operation)
	if !hasService || !hasOperation || !validService || !validOperation || !exists {
		diagnostics = append(diagnostics, inputPhaseDiagnostic(context, node, "service_ref", ir.PhaseResource, "SERVICE_BINDING_REQUIRED", "service_ref and operation have no authorized immutable service binding"))
	} else {
		product.Config["service_id"] = mustRaw(resolved.ServiceID)
		product.Config["operation"] = mustRaw(operation)
		product.Config["contract_revision"] = mustRaw(resolved.ContractRevision)
		product.Config["idempotent"] = mustRaw(resolved.Idempotent)
		product.FieldMappings["operation.config.service_id"] = "inputs.service_ref"
		product.FieldMappings["operation.config.operation"] = "inputs.operation"
		product.FieldMappings["operation.config.contract_revision"] = "inputs.operation"
		product.FieldMappings["operation.config.idempotent"] = "inputs.operation"
		if context.policy.MaxAttempts > 1 && !resolved.Idempotent {
			diagnostics = append(diagnostics, inputDiagnostic(context, node, "operation", "RPC_RETRY_POLICY_UNSAFE", "automatic business retry requires an idempotent service operation"))
		}
	}
	if request, _, exists := findInput(node, "request"); exists {
		binding, values := compileInput(context, node, request)
		diagnostics = append(diagnostics, values...)
		if !ir.HasErrors(values) {
			product.Inputs["request"] = binding
			product.FieldMappings["inputs.request"] = "inputs.request"
		}
	}
	return product, diagnostics
}

func emptyProduct(node ir.Node) NodeProduct {
	return NodeProduct{
		Config: map[string]json.RawMessage{}, Inputs: map[dsl.PortName]dsl.InputBinding{},
		Outputs: compileOutputs(node), FieldMappings: outputMappings(node),
	}
}

func compileOutputs(node ir.Node) map[dsl.PortName]dsl.DataType {
	result := make(map[dsl.PortName]dsl.DataType, len(node.Outputs))
	for _, output := range node.Outputs {
		result[dsl.PortName(output.Name)] = dsl.DataType(output.DataType)
	}
	return result
}

func outputMappings(node ir.Node) map[string]string {
	result := make(map[string]string, len(node.Outputs))
	for _, output := range node.Outputs {
		result["outputs."+string(output.Name)] = "outputs." + string(output.Name)
	}
	return result
}

func compileNamedInput(context Context, node ir.Node, name ir.PortName) (dsl.InputBinding, []ir.Diagnostic) {
	input, _, exists := findInput(node, name)
	if !exists {
		return dsl.InputBinding{}, []ir.Diagnostic{inputDiagnostic(context, node, name, "COMPILER_INPUT_MISSING", "validated node is missing a required input")}
	}
	return compileInput(context, node, input)
}

func compileInput(context Context, node ir.Node, input ir.Input) (dsl.InputBinding, []ir.Diagnostic) {
	binding := dsl.InputBinding{DataType: dsl.DataType(input.DataType)}
	switch input.Source {
	case ir.SourceLiteral:
		canonical, err := ir.CanonicalizeJSON(input.Value, ir.DefaultParseLimits)
		if err != nil {
			return dsl.InputBinding{}, []ir.Diagnostic{inputDiagnostic(context, node, input.Name, "COMPILER_LITERAL_INVALID", err.Error())}
		}
		binding.Kind = dsl.BindingLiteral
		binding.Value = canonical
	case ir.SourceRef:
		nodeID, exists := context.executionIDs[input.RefNode]
		if !exists {
			return dsl.InputBinding{}, []ir.Diagnostic{inputDiagnostic(context, node, input.Name, "COMPILER_REF_NODE_MISSING", "validated reference has no execution node id")}
		}
		binding.Kind = dsl.BindingNodeOutput
		binding.Output = &dsl.OutputReference{NodeID: nodeID, Name: dsl.PortName(input.RefOutput)}
	default:
		return dsl.InputBinding{}, []ir.Diagnostic{inputDiagnostic(context, node, input.Name, "COMPILER_INPUT_SOURCE_INVALID", "validated input has an invalid source")}
	}
	return binding, nil
}

func findInput(node ir.Node, name ir.PortName) (ir.Input, int, bool) {
	for index, input := range node.Inputs {
		if input.Name == name {
			return input, index, true
		}
	}
	return ir.Input{}, -1, false
}

func literalString(input ir.Input) (string, bool) {
	if input.Source != ir.SourceLiteral || !input.HasValue() {
		return "", false
	}
	value, _, err := ir.DecodeLiteral(input.Value)
	text, ok := value.(string)
	return text, err == nil && ok && text != ""
}

func inputDiagnostic(context Context, node ir.Node, name ir.PortName, code, message string) ir.Diagnostic {
	return inputPhaseDiagnostic(context, node, name, ir.PhaseCompile, code, message)
}

func inputPhaseDiagnostic(context Context, node ir.Node, name ir.PortName, phase ir.Phase, code, message string) ir.Diagnostic {
	_, inputIndex, exists := findInput(node, name)
	path := ir.NodePath(node.ID, context.nodeIndex) + "/inputs/" + string(name)
	if exists {
		path = ir.InputPath(node.ID, context.nodeIndex, name, inputIndex)
	}
	return ir.ErrorDiagnostic(phase, code, message, ir.Location{LogicalNodeID: node.ID, IRPath: path})
}

func mustRaw(value any) json.RawMessage {
	encoded, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return encoded
}
