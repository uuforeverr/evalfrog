package catalog

import (
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/uu999/evalfrog/internal/ir"
)

type nodeValidator func(ir.Node) []ir.Diagnostic

type contract struct {
	revision    uint32
	description NodeDescription
	runtime     RuntimeContract
	validate    nodeValidator
}

type Catalog struct {
	revision  RevisionID
	contracts map[ir.NodeType]contract
}

func BuiltinV1() *Catalog {
	contracts := []contract{
		startContract(),
		endContract(),
		branchContract(),
		codeContract(),
		httpContract(),
		rpcContract(),
	}
	catalog, err := newCatalog(BuiltinRevisionV1, contracts)
	if err != nil {
		panic(err)
	}
	return catalog
}

// BuiltinV2 keeps the v1 catalog available for immutable published snapshots.
func BuiltinV2() *Catalog {
	contracts := []contract{startContractV2(), endContractV2(), branchContractV2(), codeContract(), httpContract(), rpcContract()}
	catalog, err := newCatalog(BuiltinRevisionV2, contracts)
	if err != nil {
		panic(err)
	}
	return catalog
}

func newCatalog(revision RevisionID, contracts []contract) (*Catalog, error) {
	if revision == "" {
		return nil, fmt.Errorf("catalog revision is required")
	}
	result := &Catalog{revision: revision, contracts: make(map[ir.NodeType]contract, len(contracts))}
	for _, value := range contracts {
		if value.revision == 0 {
			return nil, fmt.Errorf("node %q has no internal contract revision", value.description.Type)
		}
		if !ir.ValidNodeType(value.description.Type) {
			return nil, fmt.Errorf("node type %q is invalid", value.description.Type)
		}
		if err := validateContractDefinition(value); err != nil {
			return nil, err
		}
		if _, exists := result.contracts[value.description.Type]; exists {
			return nil, fmt.Errorf("node type %q is registered twice", value.description.Type)
		}
		result.contracts[value.description.Type] = value
	}
	return result, nil
}

func validateContractDefinition(value contract) error {
	description := value.description
	if description.Kind != KindControl && description.Kind != KindTask {
		return fmt.Errorf("node %q has invalid kind %q", description.Type, description.Kind)
	}
	if strings.TrimSpace(description.Description) == "" || len(description.Examples) == 0 {
		return fmt.Errorf("node %q must publish a description and example", description.Type)
	}
	if value.runtime.Kind != description.Kind || value.runtime.OperationType == "" || value.runtime.OperationVersion == 0 {
		return fmt.Errorf("node %q has invalid runtime contract", description.Type)
	}
	if value.runtime.Kind == KindControl {
		policy := value.runtime.DefaultExecutionPolicy
		if policy.MaxAttempts != 0 || policy.MaxRecoveries != 0 || policy.AttemptTimeoutMS != 0 || policy.RetryBackoffMS != 0 || len(policy.RetryableErrorCodes) != 0 {
			return fmt.Errorf("control node %q cannot have an execution policy", description.Type)
		}
	} else {
		policy := value.runtime.DefaultExecutionPolicy
		if policy.MaxAttempts == 0 || policy.AttemptTimeoutMS == 0 || policy.RetryBackoffMS == 0 {
			return fmt.Errorf("task node %q has an incomplete default execution policy", description.Type)
		}
	}
	inputNames := make(map[ir.PortName]struct{}, len(description.Inputs))
	for _, input := range description.Inputs {
		if !ir.ValidPortName(input.Name) {
			return fmt.Errorf("node %q has invalid input name %q", description.Type, input.Name)
		}
		if _, exists := inputNames[input.Name]; exists {
			return fmt.Errorf("node %q describes input %q twice", description.Type, input.Name)
		}
		inputNames[input.Name] = struct{}{}
		if err := validateTypeAndSourceSet(description.Type, input.Name, input.DataTypes, input.Sources); err != nil {
			return err
		}
		if input.Constraints != nil && input.Constraints.StringPattern != "" {
			if _, err := regexp.Compile(input.Constraints.StringPattern); err != nil {
				return fmt.Errorf("node %q input %q has invalid pattern: %w", description.Type, input.Name, err)
			}
		}
	}
	if description.AdditionalInputs != nil {
		if err := validateTypeAndSourceSet(description.Type, "additional_inputs", description.AdditionalInputs.DataTypes, description.AdditionalInputs.Sources); err != nil {
			return err
		}
	}
	outputNames := make(map[ir.PortName]struct{}, len(description.Outputs.Fields))
	switch description.Outputs.Mode {
	case OutputFixed:
		for _, output := range description.Outputs.Fields {
			if !ir.ValidPortName(output.Name) || !output.DataType.Valid() {
				return fmt.Errorf("node %q has invalid fixed output %q", description.Type, output.Name)
			}
			if _, exists := outputNames[output.Name]; exists {
				return fmt.Errorf("node %q describes output %q twice", description.Type, output.Name)
			}
			outputNames[output.Name] = struct{}{}
		}
	case OutputAuthorDeclared:
		if len(description.Outputs.AllowedDataTypes) == 0 || description.Outputs.MaxFields < description.Outputs.MinFields {
			return fmt.Errorf("node %q has invalid author-declared output policy", description.Type)
		}
		for _, dataType := range description.Outputs.AllowedDataTypes {
			if !dataType.Valid() {
				return fmt.Errorf("node %q has invalid output data type %q", description.Type, dataType)
			}
		}
	default:
		return fmt.Errorf("node %q has invalid output mode %q", description.Type, description.Outputs.Mode)
	}
	for _, example := range description.Examples {
		if example.Type != description.Type {
			return fmt.Errorf("node %q publishes an example for type %q", description.Type, example.Type)
		}
	}
	return nil
}

func validateTypeAndSourceSet(nodeType ir.NodeType, inputName ir.PortName, dataTypes []ir.DataType, sources []ir.InputSource) error {
	if len(dataTypes) == 0 || len(sources) == 0 {
		return fmt.Errorf("node %q input %q must declare data types and sources", nodeType, inputName)
	}
	for _, dataType := range dataTypes {
		if !dataType.Valid() {
			return fmt.Errorf("node %q input %q has invalid data type %q", nodeType, inputName, dataType)
		}
	}
	for _, source := range sources {
		if !source.Valid() {
			return fmt.Errorf("node %q input %q has invalid source %q", nodeType, inputName, source)
		}
	}
	return nil
}

func (c *Catalog) Revision() RevisionID {
	return c.revision
}

func (c *Catalog) ContractRevision(nodeType ir.NodeType) (uint32, bool) {
	value, exists := c.contracts[nodeType]
	return value.revision, exists
}

func (c *Catalog) RuntimeContract(nodeType ir.NodeType) (RuntimeContract, bool) {
	value, exists := c.contracts[nodeType]
	if !exists {
		return RuntimeContract{}, false
	}
	result := value.runtime
	result.DefaultExecutionPolicy.RetryableErrorCodes = append([]string(nil), result.DefaultExecutionPolicy.RetryableErrorCodes...)
	return result, true
}

func (c *Catalog) Describe(nodeType ir.NodeType) (NodeDescription, bool) {
	value, exists := c.contracts[nodeType]
	if !exists {
		return NodeDescription{}, false
	}
	return cloneDescription(value.description), true
}

func (c *Catalog) Descriptions() []NodeDescription {
	result := make([]NodeDescription, 0, len(c.contracts))
	for _, value := range c.contracts {
		result = append(result, cloneDescription(value.description))
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Type < result[j].Type })
	return result
}

func cloneDescription(value NodeDescription) NodeDescription {
	encoded, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	var cloned NodeDescription
	if err := json.Unmarshal(encoded, &cloned); err != nil {
		panic(err)
	}
	return cloned
}

func (c *Catalog) ValidateNode(node ir.Node) []ir.Diagnostic {
	value, exists := c.contracts[node.Type]
	if !exists {
		return []ir.Diagnostic{ir.ErrorDiagnostic(
			ir.PhaseCatalog,
			"NODE_TYPE_UNSUPPORTED",
			fmt.Sprintf("node type %q is not present in catalog %s", node.Type, c.revision),
			ir.Location{LogicalNodeID: node.ID, IRPath: ir.NodePath(node.ID, 0) + "/type"},
		)}
	}
	diagnostics := validateDescription(node, value.description)
	if value.validate != nil {
		diagnostics = append(diagnostics, value.validate(node)...)
	}
	ir.SortDiagnostics(diagnostics)
	return diagnostics
}
