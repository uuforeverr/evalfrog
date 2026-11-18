package dsl

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/big"
	"regexp"
	"sort"
	"strings"

	sandboxcontract "github.com/uu999/evalfrog/internal/sandbox"
)

var (
	nodeIDPattern = regexp.MustCompile(`^xn_[0-9a-f]{24}$`)
	edgeIDPattern = regexp.MustCompile(`^xe_[0-9a-f]{24}$`)
	portPattern   = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_]{0,63}$`)
	routePattern  = portPattern
)

type PortRule struct {
	Required  bool
	DataTypes []DataType
}

type OperationSpec struct {
	Coordinate        Coordinate
	Kind              NodeKind
	ConfigFields      map[string]bool
	Inputs            map[PortName]PortRule
	AllowExtraInputs  bool
	Outputs           map[PortName]PortRule
	AllowExtraOutputs bool
	ValidateConfig    func(Node) []Issue
}

type Contract struct {
	version string
	specs   map[Coordinate]OperationSpec
}

func (contract Contract) Version() string {
	return contract.version
}

func NewContract(version string, specs ...OperationSpec) (Contract, error) {
	if version == "" {
		return Contract{}, fmt.Errorf("DSL contract version is required")
	}
	contract := Contract{version: version, specs: make(map[Coordinate]OperationSpec, len(specs))}
	for _, spec := range specs {
		if spec.Coordinate.Type == "" || spec.Coordinate.Version == 0 || !spec.Kind.Valid() {
			return Contract{}, fmt.Errorf("invalid operation specification %+v", spec.Coordinate)
		}
		if _, exists := contract.specs[spec.Coordinate]; exists {
			return Contract{}, fmt.Errorf("operation %s@%d is registered twice", spec.Coordinate.Type, spec.Coordinate.Version)
		}
		contract.specs[spec.Coordinate] = spec
	}
	return contract, nil
}

func BuiltinV1Contract() Contract {
	object := []DataType{TypeObject}
	allTypes := []DataType{TypeString, TypeInteger, TypeNumber, TypeBoolean, TypeObject, TypeArray}
	contract, err := NewContract(VersionV1,
		OperationSpec{
			Coordinate: Coordinate{Type: "control.start", Version: 1}, Kind: KindControl,
			ConfigFields: map[string]bool{}, Inputs: map[PortName]PortRule{},
			Outputs: map[PortName]PortRule{"workflow_input": {Required: true, DataTypes: object}},
		},
		OperationSpec{
			Coordinate: Coordinate{Type: "control.end", Version: 1}, Kind: KindControl,
			ConfigFields: map[string]bool{}, Inputs: map[PortName]PortRule{"workflow_output": {Required: true, DataTypes: object}},
			Outputs: map[PortName]PortRule{},
		},
		OperationSpec{
			Coordinate: Coordinate{Type: "control.branch", Version: 1}, Kind: KindControl,
			ConfigFields: map[string]bool{"cases": true, "default_route": true},
			Inputs:       map[PortName]PortRule{"value": {Required: true, DataTypes: allTypes}}, Outputs: map[PortName]PortRule{},
			ValidateConfig: validateBranchConfig,
		},
		OperationSpec{
			Coordinate: Coordinate{Type: "task.python", Version: 1}, Kind: KindTask,
			ConfigFields: map[string]bool{"source_code": true, "sandbox_profile": true, "model_artifact": false},
			Inputs:       map[PortName]PortRule{}, AllowExtraInputs: true,
			Outputs: map[PortName]PortRule{}, AllowExtraOutputs: true,
			ValidateConfig: validatePythonConfig,
		},
		OperationSpec{
			Coordinate: Coordinate{Type: "task.http", Version: 1}, Kind: KindTask,
			ConfigFields: map[string]bool{"connection_id": true, "method": true, "accepted_statuses": true},
			Inputs: map[PortName]PortRule{
				"relative_path": {Required: true, DataTypes: []DataType{TypeString}},
				"query":         {DataTypes: object}, "headers": {DataTypes: object}, "body": {DataTypes: allTypes},
			}, Outputs: map[PortName]PortRule{"response": {Required: true, DataTypes: object}},
			ValidateConfig: validateHTTPConfig,
		},
		OperationSpec{
			Coordinate: Coordinate{Type: "task.rpc", Version: 1}, Kind: KindTask,
			ConfigFields:   map[string]bool{"service_id": true, "operation": true, "contract_revision": true, "idempotent": true},
			Inputs:         map[PortName]PortRule{"request": {Required: true, DataTypes: object}},
			Outputs:        map[PortName]PortRule{"response": {Required: true, DataTypes: object}},
			ValidateConfig: validateRPCConfig,
		},
	)
	if err != nil {
		panic(err)
	}
	return contract
}

// BuiltinV2Contract accepts both immutable v1 snapshots and named-port v2
// control operations. The DSL envelope remains v1; operation versions carry
// the semantic change.
func BuiltinV2Contract() Contract {
	contract := BuiltinV1Contract()
	contract.specs[Coordinate{Type: "control.start", Version: 2}] = OperationSpec{
		Coordinate: Coordinate{Type: "control.start", Version: 2}, Kind: KindControl,
		ConfigFields: map[string]bool{}, Inputs: map[PortName]PortRule{},
		Outputs: map[PortName]PortRule{}, AllowExtraOutputs: true,
	}
	contract.specs[Coordinate{Type: "control.end", Version: 2}] = OperationSpec{
		Coordinate: Coordinate{Type: "control.end", Version: 2}, Kind: KindControl,
		ConfigFields: map[string]bool{}, Inputs: map[PortName]PortRule{}, AllowExtraInputs: true,
		Outputs: map[PortName]PortRule{}, AllowExtraOutputs: true,
		ValidateConfig: validateEndV2Ports,
	}
	contract.specs[Coordinate{Type: "control.branch", Version: 2}] = OperationSpec{
		Coordinate: Coordinate{Type: "control.branch", Version: 2}, Kind: KindControl,
		ConfigFields: map[string]bool{"cases": true, "default_route": true}, Inputs: map[PortName]PortRule{}, AllowExtraInputs: true,
		Outputs: map[PortName]PortRule{}, ValidateConfig: validateBranchV2Config,
	}
	return contract
}

func validateEndV2Ports(node Node) []Issue {
	if len(node.Inputs) != len(node.Outputs) {
		return []Issue{{Code: "DSL_END_OUTPUT_BINDING_MISMATCH", Message: "each end output needs a same-named input", NodeID: node.ID, Field: "outputs"}}
	}
	for name, dataType := range node.Outputs {
		if binding, exists := node.Inputs[name]; !exists || binding.DataType != dataType {
			return []Issue{{Code: "DSL_END_OUTPUT_BINDING_MISMATCH", Message: "end output type must match its input", NodeID: node.ID, Field: "outputs." + string(name)}}
		}
	}
	return nil
}

func validateBranchV2Config(node Node) []Issue {
	var fallback string
	if !decodeConfig(node, "default_route", &fallback) || !routePattern.MatchString(fallback) {
		return []Issue{{Code: "DSL_BRANCH_DEFAULT_INVALID", Message: "ELSE route is invalid", NodeID: node.ID, Field: "operation.config.default_route"}}
	}
	var cases []struct {
		Route      string `json:"route"`
		Logic      string `json:"logic"`
		Conditions []struct {
			Input    string          `json:"input"`
			Path     string          `json:"path,omitempty"`
			Operator string          `json:"operator"`
			Value    json.RawMessage `json:"value"`
		} `json:"conditions"`
	}
	if !decodeConfig(node, "cases", &cases) || len(cases) == 0 {
		return []Issue{{Code: "DSL_BRANCH_CASES_INVALID", Message: "IF cases are required", NodeID: node.ID, Field: "operation.config.cases"}}
	}
	var rawCases []map[string]json.RawMessage
	if !decodeConfig(node, "cases", &rawCases) || len(rawCases) != len(cases) {
		return []Issue{{Code: "DSL_BRANCH_CASES_INVALID", Message: "IF cases must be objects", NodeID: node.ID, Field: "operation.config.cases"}}
	}
	var issues []Issue
	for index, branchCase := range cases {
		field := fmt.Sprintf("operation.config.cases.%d", index)
		for key := range rawCases[index] {
			if key != "route" && key != "logic" && key != "conditions" {
				issues = append(issues, Issue{Code: "DSL_BRANCH_CASE_INVALID", Message: "unsupported IF case field", NodeID: node.ID, Field: field + "." + key})
			}
		}
		var rawConditions []map[string]json.RawMessage
		if json.Unmarshal(rawCases[index]["conditions"], &rawConditions) != nil || len(rawConditions) != len(branchCase.Conditions) {
			issues = append(issues, Issue{Code: "DSL_BRANCH_CASE_INVALID", Message: "conditions must be objects", NodeID: node.ID, Field: field + ".conditions"})
			continue
		}
		if !routePattern.MatchString(branchCase.Route) || branchCase.Logic != "and" && branchCase.Logic != "or" || len(branchCase.Conditions) == 0 {
			issues = append(issues, Issue{Code: "DSL_BRANCH_CASE_INVALID", Message: "case requires route, logic, and conditions", NodeID: node.ID, Field: field})
		}
		for conditionIndex, condition := range branchCase.Conditions {
			for key := range rawConditions[conditionIndex] {
				if key != "input" && key != "path" && key != "operator" && key != "value" {
					issues = append(issues, Issue{Code: "DSL_BRANCH_CASE_INVALID", Message: "unsupported IF condition field", NodeID: node.ID, Field: fmt.Sprintf("%s.conditions.%d.%s", field, conditionIndex, key)})
				}
			}
			binding, exists := node.Inputs[PortName(condition.Input)]
			if !exists || condition.Input == "" || condition.Operator == "" || len(condition.Value) == 0 {
				issues = append(issues, Issue{Code: "DSL_BRANCH_CASE_INVALID", Message: "condition requires a bound input, operator, and value", NodeID: node.ID, Field: field})
				continue
			}
			if condition.Path != "" {
				if binding.DataType != TypeObject || !validBranchPath(condition.Path) || !validDSLPathComparison(condition.Operator, condition.Value) {
					issues = append(issues, Issue{Code: "DSL_BRANCH_OPERATOR_TYPE_MISMATCH", Message: "path condition is incompatible", NodeID: node.ID, Field: field})
				}
			} else if !validDSLBranchComparison(binding.DataType, condition.Operator, condition.Value) {
				issues = append(issues, Issue{Code: "DSL_BRANCH_OPERATOR_TYPE_MISMATCH", Message: "condition is incompatible", NodeID: node.ID, Field: field})
			}
		}
	}
	return issues
}

func (contract Contract) Validate(document Document) []Issue {
	issues := make([]Issue, 0)
	if document.DSLVersion != contract.version {
		issues = append(issues, Issue{Code: "DSL_VERSION_UNSUPPORTED", Message: "dsl_version is unsupported", Field: "dsl_version"})
	}
	nodes := make(map[NodeID]Node, len(document.Nodes))
	outputs := make(map[NodeID]map[PortName]DataType, len(document.Nodes))
	startCount := 0
	endCount := 0
	for _, node := range document.Nodes {
		if !nodeIDPattern.MatchString(string(node.ID)) {
			issues = append(issues, Issue{Code: "DSL_NODE_ID_INVALID", Message: "execution node id is invalid", NodeID: node.ID, Field: "id"})
		}
		if _, exists := nodes[node.ID]; exists {
			issues = append(issues, Issue{Code: "DSL_NODE_ID_DUPLICATE", Message: "execution node id must be unique", NodeID: node.ID, Field: "id"})
		}
		nodes[node.ID] = node
		outputs[node.ID] = node.Outputs
		if node.Operation.Type == "control.start" && (node.Operation.Version == 1 || node.Operation.Version == 2) {
			startCount++
		}
		if node.Operation.Type == "control.end" && (node.Operation.Version == 1 || node.Operation.Version == 2) {
			endCount++
		}
		issues = append(issues, contract.validateNode(node)...)
	}
	if startCount != 1 {
		issues = append(issues, Issue{Code: "DSL_START_COUNT_INVALID", Message: "DSL requires exactly one control.start@1"})
	}
	if endCount != 1 {
		issues = append(issues, Issue{Code: "DSL_END_COUNT_INVALID", Message: "DSL requires exactly one control.end@1"})
	}
	if _, exists := nodes[document.EntryNodeID]; !exists {
		issues = append(issues, Issue{Code: "DSL_ENTRY_NODE_NOT_FOUND", Message: "entry_node_id does not exist", NodeID: document.EntryNodeID, Field: "entry_node_id"})
	}
	if _, exists := nodes[document.ExitNodeID]; !exists {
		issues = append(issues, Issue{Code: "DSL_EXIT_NODE_NOT_FOUND", Message: "exit_node_id does not exist", NodeID: document.ExitNodeID, Field: "exit_node_id"})
	}
	if entry, exists := nodes[document.EntryNodeID]; exists && (entry.Operation.Type != "control.start" || entry.Operation.Version != 1 && entry.Operation.Version != 2) {
		issues = append(issues, Issue{Code: "DSL_ENTRY_NODE_INVALID", Message: "entry node must be control.start@1", NodeID: entry.ID, Field: "operation"})
	}
	if exit, exists := nodes[document.ExitNodeID]; exists && (exit.Operation.Type != "control.end" || exit.Operation.Version != 1 && exit.Operation.Version != 2) {
		issues = append(issues, Issue{Code: "DSL_EXIT_NODE_INVALID", Message: "exit node must be control.end@1", NodeID: exit.ID, Field: "operation"})
	}
	issues = append(issues, validateEdgesAndGraph(document, nodes)...)
	for _, node := range document.Nodes {
		for name, input := range node.Inputs {
			if input.Kind != BindingNodeOutput || input.Output == nil {
				continue
			}
			sourceOutputs, exists := outputs[input.Output.NodeID]
			if !exists {
				issues = append(issues, Issue{Code: "DSL_INPUT_NODE_NOT_FOUND", Message: "input references a missing execution node", NodeID: node.ID, Field: "inputs." + string(name)})
				continue
			}
			sourceType, exists := sourceOutputs[input.Output.Name]
			if !exists {
				issues = append(issues, Issue{Code: "DSL_INPUT_OUTPUT_NOT_FOUND", Message: "input references a missing output", NodeID: node.ID, Field: "inputs." + string(name)})
				continue
			}
			if !compatible(sourceType, input.DataType) {
				issues = append(issues, Issue{Code: "DSL_INPUT_TYPE_MISMATCH", Message: "referenced output type is incompatible", NodeID: node.ID, Field: "inputs." + string(name)})
			}
		}
	}
	sortIssues(issues)
	return issues
}

func (contract Contract) validateNode(node Node) []Issue {
	issues := make([]Issue, 0)
	if !node.Kind.Valid() {
		issues = append(issues, Issue{Code: "DSL_NODE_KIND_INVALID", Message: "node kind is invalid", NodeID: node.ID, Field: "kind"})
	}
	spec, exists := contract.specs[node.Operation.Coordinate()]
	if !exists {
		return append(issues, Issue{Code: "DSL_OPERATION_UNKNOWN", Message: "operation is not part of the DSL contract", NodeID: node.ID, Field: "operation"})
	}
	if node.Kind != spec.Kind {
		issues = append(issues, Issue{Code: "DSL_OPERATION_KIND_MISMATCH", Message: "operation kind does not match node kind", NodeID: node.ID, Field: "kind"})
	}
	if node.Operation.Config == nil || node.Inputs == nil || node.Outputs == nil {
		issues = append(issues, Issue{Code: "DSL_NODE_CONTAINER_REQUIRED", Message: "config, inputs, and outputs must be objects", NodeID: node.ID})
		return issues
	}
	for name, required := range spec.ConfigFields {
		if _, exists := node.Operation.Config[name]; required && !exists {
			issues = append(issues, Issue{Code: "DSL_CONFIG_REQUIRED", Message: "required operation config is missing", NodeID: node.ID, Field: "operation.config." + name})
		}
	}
	for name, value := range node.Operation.Config {
		if _, exists := spec.ConfigFields[name]; !exists {
			issues = append(issues, Issue{Code: "DSL_CONFIG_UNKNOWN", Message: "operation config field is unsupported", NodeID: node.ID, Field: "operation.config." + name})
		}
		if !json.Valid(value) {
			issues = append(issues, Issue{Code: "DSL_CONFIG_JSON_INVALID", Message: "operation config is invalid JSON", NodeID: node.ID, Field: "operation.config." + name})
		}
	}
	issues = append(issues, validateInputs(node, node.Inputs, spec.Inputs, spec.AllowExtraInputs)...)
	issues = append(issues, validateOutputs(node, spec)...)
	if node.Kind == KindControl {
		if !emptyPolicy(node.ExecutionPolicy) {
			issues = append(issues, Issue{Code: "DSL_CONTROL_POLICY_FORBIDDEN", Message: "control nodes do not create attempts", NodeID: node.ID, Field: "execution_policy"})
		}
	} else {
		issues = append(issues, validateTaskPolicy(node)...)
	}
	if spec.ValidateConfig != nil {
		issues = append(issues, spec.ValidateConfig(node)...)
	}
	return issues
}

func validateInputs(node Node, actual map[PortName]InputBinding, rules map[PortName]PortRule, allowExtra bool) []Issue {
	issues := make([]Issue, 0)
	for name, rule := range rules {
		if _, exists := actual[name]; rule.Required && !exists {
			issues = append(issues, Issue{Code: "DSL_INPUT_REQUIRED", Message: "required runtime input is missing", NodeID: node.ID, Field: "inputs." + string(name)})
		}
	}
	for name, binding := range actual {
		field := "inputs." + string(name)
		if !portPattern.MatchString(string(name)) {
			issues = append(issues, Issue{Code: "DSL_INPUT_NAME_INVALID", Message: "runtime input name is invalid", NodeID: node.ID, Field: field})
		}
		rule, exists := rules[name]
		if !exists && !allowExtra {
			issues = append(issues, Issue{Code: "DSL_INPUT_UNKNOWN", Message: "runtime input is unsupported", NodeID: node.ID, Field: field})
		} else if exists && !containsType(rule.DataTypes, binding.DataType) {
			issues = append(issues, Issue{Code: "DSL_INPUT_TYPE_INVALID", Message: "runtime input data_type is unsupported", NodeID: node.ID, Field: field})
		}
		if !binding.DataType.Valid() {
			issues = append(issues, Issue{Code: "DSL_INPUT_TYPE_INVALID", Message: "runtime input data_type is invalid", NodeID: node.ID, Field: field})
		}
		switch binding.Kind {
		case BindingLiteral:
			if len(binding.Value) == 0 || binding.Output != nil || !json.Valid(binding.Value) {
				issues = append(issues, Issue{Code: "DSL_LITERAL_BINDING_INVALID", Message: "literal binding requires one valid value", NodeID: node.ID, Field: field})
			} else if actualType, valid := rawDataType(binding.Value); !valid || !compatible(actualType, binding.DataType) {
				issues = append(issues, Issue{Code: "DSL_LITERAL_TYPE_MISMATCH", Message: "literal value does not match binding data_type", NodeID: node.ID, Field: field})
			}
		case BindingNodeOutput:
			if len(binding.Value) != 0 || binding.Output == nil || binding.Output.NodeID == "" || !portPattern.MatchString(string(binding.Output.Name)) {
				issues = append(issues, Issue{Code: "DSL_OUTPUT_BINDING_INVALID", Message: "node_output binding requires one output reference", NodeID: node.ID, Field: field})
			}
		default:
			issues = append(issues, Issue{Code: "DSL_INPUT_BINDING_KIND_INVALID", Message: "input binding kind is invalid", NodeID: node.ID, Field: field})
		}
	}
	return issues
}

func validateOutputs(node Node, spec OperationSpec) []Issue {
	issues := make([]Issue, 0)
	for name, rule := range spec.Outputs {
		if _, exists := node.Outputs[name]; rule.Required && !exists {
			issues = append(issues, Issue{Code: "DSL_OUTPUT_REQUIRED", Message: "required output is missing", NodeID: node.ID, Field: "outputs." + string(name)})
		}
	}
	for name, dataType := range node.Outputs {
		field := "outputs." + string(name)
		rule, exists := spec.Outputs[name]
		if !portPattern.MatchString(string(name)) || !dataType.Valid() {
			issues = append(issues, Issue{Code: "DSL_OUTPUT_INVALID", Message: "output name or type is invalid", NodeID: node.ID, Field: field})
		}
		if !exists && !spec.AllowExtraOutputs {
			issues = append(issues, Issue{Code: "DSL_OUTPUT_UNKNOWN", Message: "output is unsupported", NodeID: node.ID, Field: field})
		} else if exists && !containsType(rule.DataTypes, dataType) {
			issues = append(issues, Issue{Code: "DSL_OUTPUT_TYPE_INVALID", Message: "output data_type is unsupported", NodeID: node.ID, Field: field})
		}
	}
	return issues
}

func validateTaskPolicy(node Node) []Issue {
	policy := node.ExecutionPolicy
	issues := make([]Issue, 0)
	if policy.MaxAttempts == 0 || policy.AttemptTimeoutMS == 0 || policy.RetryBackoff == nil {
		issues = append(issues, Issue{Code: "DSL_EXECUTION_POLICY_INVALID", Message: "task policy requires attempts, timeout, and backoff", NodeID: node.ID, Field: "execution_policy"})
		return issues
	}
	if policy.RetryBackoff.Kind != "fixed" || policy.RetryBackoff.DelayMS == 0 {
		issues = append(issues, Issue{Code: "DSL_RETRY_BACKOFF_INVALID", Message: "retry backoff must be a positive fixed delay", NodeID: node.ID, Field: "execution_policy.retry_backoff"})
	}
	if !sort.StringsAreSorted(policy.RetryableErrorCodes) {
		issues = append(issues, Issue{Code: "DSL_RETRY_CODES_UNSORTED", Message: "retryable error codes must be sorted", NodeID: node.ID, Field: "execution_policy.retryable_error_codes"})
	}
	for index := 1; index < len(policy.RetryableErrorCodes); index++ {
		if policy.RetryableErrorCodes[index] == policy.RetryableErrorCodes[index-1] {
			issues = append(issues, Issue{Code: "DSL_RETRY_CODE_DUPLICATE", Message: "retryable error codes must be unique", NodeID: node.ID, Field: "execution_policy.retryable_error_codes"})
			break
		}
	}
	return issues
}

func emptyPolicy(policy ExecutionPolicy) bool {
	return policy.MaxAttempts == 0 && policy.MaxRecoveries == 0 && policy.AttemptTimeoutMS == 0 && policy.RetryBackoff == nil && len(policy.RetryableErrorCodes) == 0
}

func validateEdgesAndGraph(document Document, nodes map[NodeID]Node) []Issue {
	issues := make([]Issue, 0)
	incoming := make(map[NodeID][]NodeID, len(nodes))
	outgoing := make(map[NodeID][]NodeID, len(nodes))
	outgoingEdges := make(map[NodeID][]Edge, len(nodes))
	edgeIDs := make(map[EdgeID]struct{}, len(document.Edges))
	for _, edge := range document.Edges {
		if !edgeIDPattern.MatchString(string(edge.ID)) {
			issues = append(issues, Issue{Code: "DSL_EDGE_ID_INVALID", Message: "execution edge id is invalid", EdgeID: edge.ID, Field: "id"})
		}
		if _, exists := edgeIDs[edge.ID]; exists {
			issues = append(issues, Issue{Code: "DSL_EDGE_ID_DUPLICATE", Message: "execution edge id must be unique", EdgeID: edge.ID, Field: "id"})
		}
		edgeIDs[edge.ID] = struct{}{}
		source, sourceExists := nodes[edge.SourceNodeID]
		_, targetExists := nodes[edge.TargetNodeID]
		if !sourceExists || !targetExists {
			issues = append(issues, Issue{Code: "DSL_EDGE_NODE_NOT_FOUND", Message: "execution edge references a missing node", EdgeID: edge.ID})
			continue
		}
		if source.Operation.Type == "control.branch" {
			if edge.Activation.Kind != ActivationRoute || !routePattern.MatchString(string(edge.Activation.Route)) {
				issues = append(issues, Issue{Code: "DSL_BRANCH_ACTIVATION_INVALID", Message: "branch edge requires a route activation", EdgeID: edge.ID, Field: "activation"})
			}
		} else if edge.Activation.Kind != ActivationAlways || edge.Activation.Route != "" {
			issues = append(issues, Issue{Code: "DSL_EDGE_ACTIVATION_INVALID", Message: "non-branch edge requires always activation", EdgeID: edge.ID, Field: "activation"})
		}
		incoming[edge.TargetNodeID] = append(incoming[edge.TargetNodeID], edge.SourceNodeID)
		outgoing[edge.SourceNodeID] = append(outgoing[edge.SourceNodeID], edge.TargetNodeID)
		outgoingEdges[edge.SourceNodeID] = append(outgoingEdges[edge.SourceNodeID], edge)
	}
	if len(nodes) == 0 {
		return append(issues, Issue{Code: "DSL_NODES_REQUIRED", Message: "DSL requires nodes"})
	}
	for id := range nodes {
		if id == document.EntryNodeID {
			if len(incoming[id]) != 0 {
				issues = append(issues, Issue{Code: "DSL_ENTRY_INCOMING_FORBIDDEN", Message: "entry node cannot have incoming edges", NodeID: id})
			}
		} else if len(incoming[id]) == 0 {
			issues = append(issues, Issue{Code: "DSL_NODE_INCOMING_REQUIRED", Message: "non-entry node requires an incoming edge", NodeID: id})
		}
		if id == document.ExitNodeID {
			if len(outgoing[id]) != 0 {
				issues = append(issues, Issue{Code: "DSL_EXIT_OUTGOING_FORBIDDEN", Message: "exit node cannot have outgoing edges", NodeID: id})
			}
		} else if len(outgoing[id]) == 0 {
			issues = append(issues, Issue{Code: "DSL_NODE_OUTGOING_REQUIRED", Message: "non-exit node requires an outgoing edge", NodeID: id})
		}
	}
	indegree := make(map[NodeID]int, len(nodes))
	for id := range nodes {
		indegree[id] = len(incoming[id])
	}
	queue := make([]NodeID, 0)
	for id, degree := range indegree {
		if degree == 0 {
			queue = append(queue, id)
		}
	}
	sort.Slice(queue, func(left, right int) bool { return queue[left] < queue[right] })
	visited := 0
	for len(queue) > 0 {
		id := queue[0]
		queue = queue[1:]
		visited++
		for _, target := range outgoing[id] {
			indegree[target]--
			if indegree[target] == 0 {
				queue = append(queue, target)
				sort.Slice(queue, func(left, right int) bool { return queue[left] < queue[right] })
			}
		}
	}
	if visited != len(nodes) {
		issues = append(issues, Issue{Code: "DSL_GRAPH_CYCLE", Message: "DSL control graph must be acyclic"})
	}
	reachableFromEntry := reachableNodes(document.EntryNodeID, outgoing)
	reachesExit := reachableNodes(document.ExitNodeID, incoming)
	for id := range nodes {
		if !reachableFromEntry[id] {
			issues = append(issues, Issue{Code: "DSL_NODE_NOT_REACHABLE", Message: "node is not reachable from entry", NodeID: id})
		}
		if !reachesExit[id] {
			issues = append(issues, Issue{Code: "DSL_NODE_CANNOT_REACH_EXIT", Message: "node cannot reach exit", NodeID: id})
		}
		if nodes[id].Operation.Type == "control.branch" {
			issues = append(issues, validateDSLBranchRoutes(nodes[id], outgoingEdges[id])...)
		}
	}
	return issues
}

func reachableNodes(start NodeID, outgoing map[NodeID][]NodeID) map[NodeID]bool {
	result := map[NodeID]bool{start: true}
	stack := []NodeID{start}
	for len(stack) > 0 {
		id := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		for _, next := range outgoing[id] {
			if !result[next] {
				result[next] = true
				stack = append(stack, next)
			}
		}
	}
	return result
}

func validateDSLBranchRoutes(node Node, edges []Edge) []Issue {
	declared := make(map[RouteName]bool)
	var defaultRoute RouteName
	_ = json.Unmarshal(node.Operation.Config["default_route"], &defaultRoute)
	declared[defaultRoute] = false
	var cases []struct {
		Route RouteName `json:"route"`
	}
	_ = json.Unmarshal(node.Operation.Config["cases"], &cases)
	for _, branchCase := range cases {
		declared[branchCase.Route] = false
	}
	issues := make([]Issue, 0)
	for _, edge := range edges {
		if _, exists := declared[edge.Activation.Route]; !exists {
			issues = append(issues, Issue{Code: "DSL_BRANCH_ROUTE_UNDECLARED", Message: "branch edge route is not declared", EdgeID: edge.ID, Field: "activation.route"})
		} else {
			declared[edge.Activation.Route] = true
		}
	}
	for route, used := range declared {
		if route != "" && !used {
			issues = append(issues, Issue{Code: "DSL_BRANCH_ROUTE_UNUSED", Message: "declared branch route has no outgoing edge", NodeID: node.ID, Field: "operation.config.cases"})
		}
	}
	return issues
}

func validateBranchConfig(node Node) []Issue {
	issues := make([]Issue, 0)
	var defaultRoute string
	if !decodeConfig(node, "default_route", &defaultRoute) || !routePattern.MatchString(defaultRoute) {
		issues = append(issues, Issue{Code: "DSL_BRANCH_DEFAULT_INVALID", Message: "branch default_route is invalid", NodeID: node.ID, Field: "operation.config.default_route"})
	}
	var cases []map[string]json.RawMessage
	if !decodeConfig(node, "cases", &cases) || len(cases) == 0 {
		issues = append(issues, Issue{Code: "DSL_BRANCH_CASES_INVALID", Message: "branch cases must be a non-empty array", NodeID: node.ID, Field: "operation.config.cases"})
		return issues
	}
	for index, branchCase := range cases {
		var route, operator, path string
		if raw, exists := branchCase["route"]; !exists || json.Unmarshal(raw, &route) != nil || !routePattern.MatchString(route) {
			issues = append(issues, Issue{Code: "DSL_BRANCH_CASE_INVALID", Message: "branch case route is invalid", NodeID: node.ID, Field: fmt.Sprintf("operation.config.cases.%d.route", index)})
		}
		if raw, exists := branchCase["operator"]; !exists || json.Unmarshal(raw, &operator) != nil || operator == "" {
			issues = append(issues, Issue{Code: "DSL_BRANCH_CASE_INVALID", Message: "branch case operator is invalid", NodeID: node.ID, Field: fmt.Sprintf("operation.config.cases.%d.operator", index)})
		}
		comparison, hasComparison := branchCase["value"]
		if !hasComparison {
			issues = append(issues, Issue{Code: "DSL_BRANCH_CASE_INVALID", Message: "branch case value is required", NodeID: node.ID, Field: fmt.Sprintf("operation.config.cases.%d.value", index)})
		}
		pathRaw, hasPath := branchCase["path"]
		if hasPath && (json.Unmarshal(pathRaw, &path) != nil || !validBranchPath(path)) {
			issues = append(issues, Issue{Code: "DSL_BRANCH_CASE_INVALID", Message: "branch case path is invalid", NodeID: node.ID, Field: fmt.Sprintf("operation.config.cases.%d.path", index)})
		}
		valueBinding := node.Inputs["value"]
		if hasPath {
			if valueBinding.DataType != TypeObject || !validDSLPathComparison(operator, comparison) {
				issues = append(issues, Issue{Code: "DSL_BRANCH_OPERATOR_TYPE_MISMATCH", Message: "path operator or comparison value is incompatible", NodeID: node.ID, Field: fmt.Sprintf("operation.config.cases.%d", index)})
			}
		} else if !validDSLBranchComparison(valueBinding.DataType, operator, comparison) {
			issues = append(issues, Issue{Code: "DSL_BRANCH_OPERATOR_TYPE_MISMATCH", Message: "operator or comparison value is incompatible with branch value", NodeID: node.ID, Field: fmt.Sprintf("operation.config.cases.%d", index)})
		}
		for field := range branchCase {
			if field != "route" && field != "path" && field != "operator" && field != "value" {
				issues = append(issues, Issue{Code: "DSL_BRANCH_CASE_INVALID", Message: "branch case field is unsupported", NodeID: node.ID, Field: fmt.Sprintf("operation.config.cases.%d.%s", index, field)})
			}
		}
	}
	return issues
}

func validBranchPath(path string) bool {
	if path == "" {
		return false
	}
	for _, segment := range strings.Split(path, ".") {
		if !portPattern.MatchString(segment) {
			return false
		}
	}
	return true
}

var dslBranchOperators = map[DataType]map[string]bool{
	TypeString:  {"eq": true, "neq": true, "contains": true, "not_contains": true, "starts_with": true, "ends_with": true},
	TypeInteger: {"eq": true, "neq": true, "gt": true, "gte": true, "lt": true, "lte": true},
	TypeNumber:  {"eq": true, "neq": true, "gt": true, "gte": true, "lt": true, "lte": true},
	TypeBoolean: {"eq": true, "neq": true},
	TypeArray:   {"eq": true, "neq": true, "contains": true, "not_contains": true},
	TypeObject:  {"eq": true, "neq": true, "has_key": true, "not_has_key": true},
}

func validDSLBranchComparison(valueType DataType, operator string, raw json.RawMessage) bool {
	if !dslBranchOperators[valueType][operator] {
		return false
	}
	comparisonType, valid := rawDataType(raw)
	switch valueType {
	case TypeString:
		return valid && comparisonType == TypeString
	case TypeInteger, TypeNumber:
		return valid && (comparisonType == TypeInteger || comparisonType == TypeNumber)
	case TypeBoolean:
		return valid && comparisonType == TypeBoolean
	case TypeArray:
		return operator == "contains" || operator == "not_contains" || valid && comparisonType == TypeArray
	case TypeObject:
		if operator == "has_key" || operator == "not_has_key" {
			return valid && comparisonType == TypeString
		}
		return valid && comparisonType == TypeObject
	default:
		return false
	}
}

func validDSLPathComparison(operator string, raw json.RawMessage) bool {
	comparisonType, valid := rawDataType(raw)
	switch operator {
	case "starts_with", "ends_with", "has_key", "not_has_key":
		return valid && comparisonType == TypeString
	case "gt", "gte", "lt", "lte":
		return valid && (comparisonType == TypeInteger || comparisonType == TypeNumber)
	case "contains", "not_contains":
		return true
	case "eq", "neq":
		return valid
	default:
		return false
	}
}

func rawDataType(raw json.RawMessage) (DataType, bool) {
	if !json.Valid(raw) || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return "", false
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if decoder.Decode(&value) != nil {
		return "", false
	}
	switch typed := value.(type) {
	case string:
		return TypeString, true
	case bool:
		return TypeBoolean, true
	case json.Number:
		rational, ok := new(big.Rat).SetString(typed.String())
		if ok && rational.IsInt() {
			return TypeInteger, true
		}
		return TypeNumber, true
	case []any:
		return TypeArray, true
	case map[string]any:
		return TypeObject, true
	default:
		return "", false
	}
}

func validatePythonConfig(node Node) []Issue {
	issues := make([]Issue, 0)
	var source, sandbox string
	if !decodeConfig(node, "source_code", &source) || source == "" {
		issues = append(issues, Issue{Code: "DSL_PYTHON_SOURCE_INVALID", Message: "python source_code is required", NodeID: node.ID, Field: "operation.config.source_code"})
	} else if sourceError := sandboxcontract.ValidateSource(source); sourceError != nil {
		issues = append(issues, Issue{Code: sourceError.Code, Message: sourceError.Message, NodeID: node.ID, Field: sandboxcontract.SourceField})
	}
	if !decodeConfig(node, "sandbox_profile", &sandbox) || sandbox == "" {
		issues = append(issues, Issue{Code: "DSL_SANDBOX_PROFILE_INVALID", Message: "sandbox profile is required", NodeID: node.ID, Field: "operation.config.sandbox_profile"})
	}
	return issues
}

func validateHTTPConfig(node Node) []Issue {
	issues := make([]Issue, 0)
	var connectionID, method string
	if !decodeConfig(node, "connection_id", &connectionID) || connectionID == "" {
		issues = append(issues, Issue{Code: "DSL_HTTP_CONNECTION_INVALID", Message: "connection_id is required", NodeID: node.ID, Field: "operation.config.connection_id"})
	}
	if !decodeConfig(node, "method", &method) || !allowedHTTPMethod(method) {
		issues = append(issues, Issue{Code: "DSL_HTTP_METHOD_INVALID", Message: "method is required", NodeID: node.ID, Field: "operation.config.method"})
	}
	var statuses []int
	if !decodeConfig(node, "accepted_statuses", &statuses) || len(statuses) == 0 {
		issues = append(issues, Issue{Code: "DSL_HTTP_STATUSES_INVALID", Message: "accepted_statuses must be a non-empty integer array", NodeID: node.ID, Field: "operation.config.accepted_statuses"})
	} else {
		seen := make(map[int]bool, len(statuses))
		for _, status := range statuses {
			if status < 100 || status > 599 || seen[status] {
				issues = append(issues, Issue{Code: "DSL_HTTP_STATUSES_INVALID", Message: "accepted_statuses must contain unique HTTP status codes", NodeID: node.ID, Field: "operation.config.accepted_statuses"})
				break
			}
			seen[status] = true
		}
	}
	return issues
}

func allowedHTTPMethod(value string) bool {
	switch value {
	case "GET", "POST", "PUT", "PATCH", "DELETE":
		return true
	default:
		return false
	}
}

func validateRPCConfig(node Node) []Issue {
	issues := make([]Issue, 0)
	for _, field := range []string{"service_id", "operation", "contract_revision"} {
		var value string
		if !decodeConfig(node, field, &value) || value == "" {
			issues = append(issues, Issue{Code: "DSL_RPC_CONFIG_INVALID", Message: "RPC config string is required", NodeID: node.ID, Field: "operation.config." + field})
		}
	}
	var idempotent bool
	if !decodeConfig(node, "idempotent", &idempotent) {
		issues = append(issues, Issue{Code: "DSL_RPC_CONFIG_INVALID", Message: "RPC idempotent flag is required", NodeID: node.ID, Field: "operation.config.idempotent"})
	}
	return issues
}

func decodeConfig(node Node, field string, target any) bool {
	raw, exists := node.Operation.Config[field]
	if !exists || !json.Valid(raw) {
		return false
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	return decoder.Decode(target) == nil
}

func containsType(values []DataType, target DataType) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
