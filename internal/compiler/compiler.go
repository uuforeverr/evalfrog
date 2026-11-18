package compiler

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/uu999/evalfrog/internal/catalog"
	"github.com/uu999/evalfrog/internal/dsl"
	"github.com/uu999/evalfrog/internal/ir"
	"github.com/uu999/evalfrog/internal/sourcemap"
)

const maxCompiledArtifactBytes = 16 << 20

type Compiler struct {
	version       string
	registry      Registry
	dslContract   dsl.Contract
	compatibility dsl.CompatibilityChecker
}

func New(version string, registry Registry, contract dsl.Contract, compatibility dsl.CompatibilityChecker) (Compiler, error) {
	if version == "" {
		return Compiler{}, fmt.Errorf("compiler version is required")
	}
	if contract.Version() == "" {
		return Compiler{}, fmt.Errorf("DSL contract is required")
	}
	return Compiler{version: version, registry: registry, dslContract: contract, compatibility: compatibility}, nil
}

func BuiltinV1() Compiler {
	compiler, err := New(VersionV1, BuiltinRegistry(), dsl.BuiltinV1Contract(), dsl.BuiltinV1Compatibility())
	if err != nil {
		panic(err)
	}
	return compiler
}

func BuiltinV2() Compiler {
	compiler, err := New(VersionV2, BuiltinRegistryV2(), dsl.BuiltinV2Contract(), dsl.BuiltinV2Compatibility())
	if err != nil {
		panic(err)
	}
	return compiler
}

func (compiler Compiler) Compile(request Request) (Result, []ir.Diagnostic) {
	if request.Catalog == nil {
		return Result{}, []ir.Diagnostic{ir.ErrorDiagnostic(ir.PhaseCompile, "CATALOG_REQUIRED", "compiler requires an immutable node catalog", ir.Location{})}
	}
	if request.Policy.Revision() == "" {
		return Result{}, []ir.Diagnostic{ir.ErrorDiagnostic(ir.PhaseCompile, "POLICY_REQUIRED", "compiler requires an immutable project policy", ir.Location{})}
	}
	if compiler.version == VersionV1 && request.IR.IRVersion != ir.VersionV1 {
		return Result{}, []ir.Diagnostic{ir.ErrorDiagnostic(ir.PhaseCompile, "IR_VERSION_UNSUPPORTED", "compiler-v1 accepts only IR v1", ir.Location{IRPath: "/ir_version"})}
	}
	if compiler.version == VersionV2 && (request.IR.IRVersion == ir.VersionV1 && request.Catalog.Revision() != catalog.BuiltinRevisionV1 || request.IR.IRVersion == ir.VersionV2 && request.Catalog.Revision() != catalog.BuiltinRevisionV2) {
		return Result{}, []ir.Diagnostic{ir.ErrorDiagnostic(ir.PhaseCompile, "IR_CATALOG_VERSION_MISMATCH", "IR version does not match its immutable catalog revision", ir.Location{IRPath: "/ir_version"})}
	}
	validator := ir.NewStrictValidator(request.Catalog)
	if diagnostics := validator.Validate(request.IR); ir.HasErrors(diagnostics) {
		return Result{}, diagnostics
	}
	graph, diagnostics := analyzeControlGraph(request.IR)
	if ir.HasErrors(diagnostics) {
		return Result{}, diagnostics
	}
	if diagnostics = validateDataBindings(request.IR, graph); ir.HasErrors(diagnostics) {
		return Result{}, diagnostics
	}
	executionNodes, executionEdges, diagnostics := buildExecutionIDs(request.IR)
	if ir.HasErrors(diagnostics) {
		return Result{}, diagnostics
	}
	manifest := Manifest{CompilerVersion: compiler.version, CatalogRevision: string(request.Catalog.Revision()), PolicyRevision: request.Policy.Revision()}
	result := Result{
		Manifest: manifest,
		DSL: dsl.Document{
			DSLVersion: dsl.VersionV1, EntryNodeID: executionNodes[graph.start], ExitNodeID: executionNodes[graph.end],
			Nodes: []dsl.Node{}, Edges: []dsl.Edge{},
		},
		SourceMap: sourcemap.Document{
			SourceMapVersion: sourcemap.VersionV1,
			Nodes:            make(map[dsl.NodeID]string, len(request.IR.Nodes)), Edges: make(map[dsl.EdgeID]string, len(request.IR.Edges)),
			Fields: make(map[dsl.NodeID]map[string]string, len(request.IR.Nodes)),
		},
	}
	diagnostics = make([]ir.Diagnostic, 0)
	registry := compiler.registry
	if compiler.version == VersionV2 && request.IR.IRVersion == ir.VersionV1 {
		registry = BuiltinRegistry()
	}
	for nodeIndex, node := range request.IR.Nodes {
		handler, exists := registry.Handler(node.Type)
		if !exists {
			diagnostics = append(diagnostics, ir.ErrorDiagnostic(ir.PhaseCompile, "COMPILER_HANDLER_NOT_REGISTERED", "node type has no compiler handler", ir.Location{LogicalNodeID: node.ID, IRPath: ir.NodePath(node.ID, nodeIndex)}))
			continue
		}
		runtimeContract, exists := request.Catalog.RuntimeContract(node.Type)
		if !exists || !handlerMatchesCatalog(handler, runtimeContract) {
			diagnostics = append(diagnostics, ir.ErrorDiagnostic(ir.PhaseCompile, "COMPILER_CATALOG_MISMATCH", "compiler handler does not match the catalog runtime contract", ir.Location{LogicalNodeID: node.ID, IRPath: ir.NodePath(node.ID, nodeIndex)}))
			continue
		}
		policy, policyErr := request.Policy.resolve(node.Type, runtimeContract.DefaultExecutionPolicy, runtimeContract.Kind)
		if policyErr != nil {
			diagnostics = append(diagnostics, ir.ErrorDiagnostic(ir.PhaseCompile, "EXECUTION_POLICY_INVALID", policyErr.Error(), ir.Location{LogicalNodeID: node.ID, IRPath: ir.NodePath(node.ID, nodeIndex)}))
			continue
		}
		context := Context{
			executionNodeID: executionNodes[node.ID], executionIDs: executionNodes, policy: policy,
			resources: request.Resources, nodeIndex: nodeIndex,
		}
		product, values := handler.Compile(context, node)
		diagnostics = append(diagnostics, values...)
		if ir.HasErrors(values) {
			continue
		}
		if product.Config == nil || product.Inputs == nil || product.Outputs == nil || product.FieldMappings == nil {
			diagnostics = append(diagnostics, ir.ErrorDiagnostic(ir.PhaseCompile, "COMPILER_HANDLER_PRODUCT_INVALID", "compiler handler returned nil product containers", ir.Location{LogicalNodeID: node.ID, IRPath: ir.NodePath(node.ID, nodeIndex)}))
			continue
		}
		executionNode := dsl.Node{
			ID: executionNodes[node.ID], Kind: handler.Kind(),
			Operation: dsl.Operation{Type: handler.Coordinate().Type, Version: handler.Coordinate().Version, Config: cloneRawMap(product.Config)},
			Inputs:    cloneInputs(product.Inputs), Outputs: cloneOutputs(product.Outputs), ExecutionPolicy: policy,
		}
		result.DSL.Nodes = append(result.DSL.Nodes, executionNode)
		result.SourceMap.Nodes[executionNode.ID] = string(node.ID)
		result.SourceMap.Fields[executionNode.ID] = cloneStringMap(product.FieldMappings)
	}
	if ir.HasErrors(diagnostics) {
		ir.SortDiagnostics(diagnostics)
		return Result{}, ir.LimitDiagnostics(diagnostics)
	}
	for _, edge := range request.IR.Edges {
		activation := dsl.Activation{Kind: dsl.ActivationAlways}
		if graph.nodes[edge.Source].Type == "branch" {
			activation = dsl.Activation{Kind: dsl.ActivationRoute, Route: dsl.RouteName(edge.Route)}
		}
		executionEdge := dsl.Edge{
			ID: executionEdges[edge.ID], SourceNodeID: executionNodes[edge.Source], TargetNodeID: executionNodes[edge.Target], Activation: activation,
		}
		result.DSL.Edges = append(result.DSL.Edges, executionEdge)
		result.SourceMap.Edges[executionEdge.ID] = string(edge.ID)
	}
	sort.Slice(result.DSL.Nodes, func(left, right int) bool { return result.DSL.Nodes[left].ID < result.DSL.Nodes[right].ID })
	sort.Slice(result.DSL.Edges, func(left, right int) bool { return result.DSL.Edges[left].ID < result.DSL.Edges[right].ID })
	if issues := compiler.dslContract.Validate(result.DSL); len(issues) != 0 {
		return Result{}, dslIssuesToDiagnostics(issues, result.SourceMap)
	}
	if issues := compiler.compatibility.CheckAll(result.DSL); len(issues) != 0 {
		return Result{}, dslIssuesToDiagnostics(issues, result.SourceMap)
	}
	if diagnostics = ValidateSourceMap(result.DSL, result.SourceMap); ir.HasErrors(diagnostics) {
		return Result{}, diagnostics
	}
	if diagnostics = finalizeHashes(request.IR, &result); ir.HasErrors(diagnostics) {
		return Result{}, diagnostics
	}
	return result, nil
}

func handlerMatchesCatalog(handler Handler, runtime catalog.RuntimeContract) bool {
	kind := dsl.KindControl
	if runtime.Kind == catalog.KindTask {
		kind = dsl.KindTask
	}
	coordinate := handler.Coordinate()
	return handler.Kind() == kind && coordinate.Type == runtime.OperationType && coordinate.Version == runtime.OperationVersion
}

func buildExecutionIDs(document ir.Document) (map[ir.LogicalID]dsl.NodeID, map[ir.LogicalID]dsl.EdgeID, []ir.Diagnostic) {
	nodes := make(map[ir.LogicalID]dsl.NodeID, len(document.Nodes))
	edges := make(map[ir.LogicalID]dsl.EdgeID, len(document.Edges))
	seenNodes := make(map[dsl.NodeID]ir.LogicalID, len(document.Nodes))
	seenEdges := make(map[dsl.EdgeID]ir.LogicalID, len(document.Edges))
	diagnostics := make([]ir.Diagnostic, 0)
	for index, node := range document.Nodes {
		id := dsl.NodeID("xn_" + deterministicSuffix("node", string(node.ID)))
		if previous, exists := seenNodes[id]; exists && previous != node.ID {
			diagnostics = append(diagnostics, ir.ErrorDiagnostic(ir.PhaseCompile, "EXECUTION_NODE_ID_COLLISION", "deterministic execution node id collided", ir.Location{LogicalNodeID: node.ID, IRPath: ir.NodePath(node.ID, index)}))
		}
		nodes[node.ID] = id
		seenNodes[id] = node.ID
	}
	for index, edge := range document.Edges {
		id := dsl.EdgeID("xe_" + deterministicSuffix("edge", string(edge.ID)))
		if previous, exists := seenEdges[id]; exists && previous != edge.ID {
			diagnostics = append(diagnostics, ir.ErrorDiagnostic(ir.PhaseCompile, "EXECUTION_EDGE_ID_COLLISION", "deterministic execution edge id collided", ir.Location{LogicalEdgeID: edge.ID, IRPath: ir.EdgePath(edge.ID, index)}))
		}
		edges[edge.ID] = id
		seenEdges[id] = edge.ID
	}
	ir.SortDiagnostics(diagnostics)
	return nodes, edges, diagnostics
}

func deterministicSuffix(kind, logicalID string) string {
	digest := sha256.Sum256([]byte("evalfrog\x00dsl-v1\x00" + kind + "\x00" + logicalID))
	return hex.EncodeToString(digest[:12])
}

func finalizeHashes(document ir.Document, result *Result) []ir.Diagnostic {
	canonicalIR, irHash, err := ir.CanonicalDocumentHash(document)
	if err != nil {
		return []ir.Diagnostic{ir.ErrorDiagnostic(ir.PhaseCanonical, "IR_CANONICALIZATION_FAILED", err.Error(), ir.Location{})}
	}
	canonicalDSL, dslHash, err := canonicalHash(result.DSL)
	if err != nil {
		return []ir.Diagnostic{ir.ErrorDiagnostic(ir.PhaseCanonical, "DSL_CANONICALIZATION_FAILED", err.Error(), ir.Location{})}
	}
	canonicalSourceMap, sourceMapHash, err := canonicalHash(result.SourceMap)
	if err != nil {
		return []ir.Diagnostic{ir.ErrorDiagnostic(ir.PhaseCanonical, "SOURCE_MAP_CANONICALIZATION_FAILED", err.Error(), ir.Location{})}
	}
	envelope := struct {
		IRHash           string   `json:"ir_hash"`
		DSLHash          string   `json:"dsl_hash"`
		SourceMapHash    string   `json:"source_map_hash"`
		CompilerManifest Manifest `json:"compiler_manifest"`
	}{IRHash: irHash, DSLHash: dslHash, SourceMapHash: sourceMapHash, CompilerManifest: result.Manifest}
	_, definitionHash, err := canonicalHash(envelope)
	if err != nil {
		return []ir.Diagnostic{ir.ErrorDiagnostic(ir.PhaseCanonical, "DEFINITION_HASH_FAILED", err.Error(), ir.Location{})}
	}
	result.CanonicalIR = canonicalIR
	result.CanonicalDSL = canonicalDSL
	result.CanonicalSourceMap = canonicalSourceMap
	result.Hashes = Hashes{IRHash: irHash, DSLHash: dslHash, SourceMapHash: sourceMapHash, DefinitionHash: definitionHash}
	return nil
}

func canonicalHash(value any) ([]byte, string, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, "", err
	}
	limits := ir.DefaultParseLimits
	limits.MaxDocumentBytes = maxCompiledArtifactBytes
	canonical, err := ir.CanonicalizeJSON(encoded, limits)
	if err != nil {
		return nil, "", err
	}
	return canonical, ir.HashCanonical(canonical), nil
}

func cloneRawMap(values map[string]json.RawMessage) map[string]json.RawMessage {
	result := make(map[string]json.RawMessage, len(values))
	for key, value := range values {
		result[key] = append(json.RawMessage(nil), value...)
	}
	return result
}

func cloneInputs(values map[dsl.PortName]dsl.InputBinding) map[dsl.PortName]dsl.InputBinding {
	result := make(map[dsl.PortName]dsl.InputBinding, len(values))
	for key, value := range values {
		copy := value
		copy.Value = append(json.RawMessage(nil), value.Value...)
		if value.Output != nil {
			output := *value.Output
			copy.Output = &output
		}
		result[key] = copy
	}
	return result
}

func cloneOutputs(values map[dsl.PortName]dsl.DataType) map[dsl.PortName]dsl.DataType {
	result := make(map[dsl.PortName]dsl.DataType, len(values))
	for key, value := range values {
		result[key] = value
	}
	return result
}

func cloneStringMap(values map[string]string) map[string]string {
	result := make(map[string]string, len(values))
	for key, value := range values {
		result[key] = value
	}
	return result
}

func dslIssuesToDiagnostics(issues []dsl.Issue, sourceMap sourcemap.Document) []ir.Diagnostic {
	diagnostics := make([]ir.Diagnostic, 0, len(issues))
	for _, issue := range issues {
		location := ir.Location{}
		if logicalID := sourceMap.Nodes[issue.NodeID]; logicalID != "" {
			location.LogicalNodeID = ir.LogicalID(logicalID)
			location.IRPath = sourceMap.Fields[issue.NodeID][issue.Field]
		}
		if logicalID := sourceMap.Edges[issue.EdgeID]; logicalID != "" {
			location.LogicalEdgeID = ir.LogicalID(logicalID)
		}
		diagnostic := ir.ErrorDiagnostic(ir.PhaseDSL, issue.Code, issue.Message, location)
		diagnostic.Details = map[string]any{"execution_node_id": issue.NodeID, "execution_edge_id": issue.EdgeID, "dsl_field": issue.Field}
		diagnostics = append(diagnostics, diagnostic)
	}
	ir.SortDiagnostics(diagnostics)
	return ir.LimitDiagnostics(diagnostics)
}

func ValidateSourceMap(document dsl.Document, sourceMap sourcemap.Document) []ir.Diagnostic {
	required := requiredSourceFields(document)
	issues := sourcemap.Validate(sourceMap, document, required)
	diagnostics := make([]ir.Diagnostic, 0, len(issues))
	for _, issue := range issues {
		location := ir.Location{}
		if logicalID := sourceMap.Nodes[issue.NodeID]; logicalID != "" {
			location.LogicalNodeID = ir.LogicalID(logicalID)
			location.IRPath = sourceMap.Fields[issue.NodeID][issue.Field]
		}
		if logicalID := sourceMap.Edges[issue.EdgeID]; logicalID != "" {
			location.LogicalEdgeID = ir.LogicalID(logicalID)
		}
		diagnostic := ir.ErrorDiagnostic(ir.PhaseSourceMap, issue.Code, issue.Message, location)
		diagnostic.Details = map[string]any{"execution_node_id": issue.NodeID, "execution_edge_id": issue.EdgeID, "dsl_field": issue.Field}
		diagnostics = append(diagnostics, diagnostic)
	}
	ir.SortDiagnostics(diagnostics)
	return ir.LimitDiagnostics(diagnostics)
}

func requiredSourceFields(document dsl.Document) sourcemap.RequiredFields {
	result := make(sourcemap.RequiredFields, len(document.Nodes))
	for _, node := range document.Nodes {
		fields := make([]string, 0, len(node.Inputs)+len(node.Outputs)+len(node.Operation.Config))
		for name := range node.Inputs {
			fields = append(fields, "inputs."+string(name))
		}
		for name := range node.Outputs {
			fields = append(fields, "outputs."+string(name))
		}
		for name, raw := range node.Operation.Config {
			if node.Operation.Type == "task.python" && name == "sandbox_profile" {
				continue
			}
			root := "operation.config." + name
			fields = append(fields, root)
			if node.Operation.Type == "control.branch" && name == "cases" {
				var cases []map[string]json.RawMessage
				if json.Unmarshal(raw, &cases) == nil {
					for index, branchCase := range cases {
						for field := range branchCase {
							fields = append(fields, fmt.Sprintf("%s.%d.%s", root, index, field))
						}
						if node.Operation.Version == 2 {
							var conditions []map[string]json.RawMessage
							if json.Unmarshal(branchCase["conditions"], &conditions) == nil {
								for conditionIndex, condition := range conditions {
									for field := range condition {
										fields = append(fields, fmt.Sprintf("%s.%d.conditions.%d.%s", root, index, conditionIndex, field))
									}
								}
							}
						}
					}
				}
			}
			if node.Operation.Type == "task.python" && name == "model_artifact" {
				fields = append(fields, root+".id", root+".digest")
			}
		}
		sort.Strings(fields)
		result[node.ID] = uniqueStrings(fields)
	}
	return result
}
