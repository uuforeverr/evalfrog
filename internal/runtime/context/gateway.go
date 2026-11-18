// Package context owns read-only execution-context assembly for workers.
// MySQL remains authoritative; only immutable parsed Snapshots use process L1.
package context

import (
	stdcontext "context"
	"encoding/json"
	"fmt"

	"github.com/uu999/evalfrog/internal/dsl"
	"github.com/uu999/evalfrog/internal/scheduling"
)

type LoadCommand struct {
	ProjectID, RunID, AttemptID, LeaseToken string
	AttemptSequence                         uint32
	FencingToken                            uint64
}

type LoadResult struct {
	Context ExecutionContext
	Err     error
}

type Metadata struct {
	ProjectID, RunID, NodeRunID, ExecutionNodeID, AttemptID, SnapshotID string
	AttemptSequence                                                     uint32
	Operation                                                           dsl.Coordinate
	ResourceClass                                                       scheduling.ResourceClass
	ResolvedInputs                                                      map[string]json.RawMessage
}

type MetadataResult struct {
	Metadata Metadata
	Err      error
}

type ExecutionContext struct {
	ContextVersion   int                           `json:"context_version"`
	ProjectID        string                        `json:"project_id"`
	RunID            string                        `json:"run_id"`
	NodeRunID        string                        `json:"node_run_id"`
	ExecutionNodeID  string                        `json:"execution_node_id"`
	AttemptID        string                        `json:"attempt_id"`
	AttemptSequence  uint32                        `json:"attempt_sequence"`
	LeaseToken       string                        `json:"-"`
	FencingToken     uint64                        `json:"-"`
	SnapshotID       string                        `json:"snapshot_id"`
	ResourceClass    scheduling.ResourceClass      `json:"resource_class"`
	Operation        dsl.Operation                 `json:"operation"`
	ExecutionPolicy  dsl.ExecutionPolicy           `json:"execution_policy"`
	OutputContract   map[dsl.PortName]dsl.DataType `json:"output_contract"`
	WorkflowInput    json.RawMessage               `json:"workflow_input"`
	Inputs           map[string]json.RawMessage    `json:"inputs"`
	UpstreamOutputs  map[string]json.RawMessage    `json:"upstream_outputs"`
	ResourceMaterial map[string]json.RawMessage    `json:"resource_material"`
}

type Repository interface {
	LoadAttemptMetadataBatch(stdcontext.Context, []LoadCommand) []MetadataResult
	LoadSnapshotDSL(stdcontext.Context, string, string) (json.RawMessage, error)
	LoadRunContext(stdcontext.Context, string, string, []string) (json.RawMessage, map[string]json.RawMessage, error)
}

type Gateway struct {
	repository Repository
	snapshots  *SnapshotCache
}

func NewGateway(repository Repository, snapshots *SnapshotCache) (*Gateway, error) {
	if repository == nil || snapshots == nil {
		return nil, fmt.Errorf("execution context repository and snapshot L1 are required")
	}
	return &Gateway{repository: repository, snapshots: snapshots}, nil
}

func (gateway *Gateway) Load(ctx stdcontext.Context, command LoadCommand) (ExecutionContext, error) {
	result := gateway.LoadBatch(ctx, []LoadCommand{command})[0]
	return result.Context, result.Err
}

func (gateway *Gateway) LoadBatch(ctx stdcontext.Context, commands []LoadCommand) []LoadResult {
	results := make([]LoadResult, len(commands))
	metadata := make([]Metadata, len(commands))
	snapshots := make([]*parsedSnapshot, len(commands))
	type runGroup struct {
		projectID string
		runID     string
		items     []int
		outputs   map[string]struct{}
		input     json.RawMessage
		values    map[string]json.RawMessage
		err       error
	}
	groups := make(map[string]*runGroup)
	for index, command := range commands {
		if err := validateLoadCommand(command); err != nil {
			results[index].Err = err
			continue
		}
	}
	validCommands := make([]LoadCommand, 0, len(commands))
	validIndexes := make([]int, 0, len(commands))
	for index, command := range commands {
		if results[index].Err == nil {
			validCommands = append(validCommands, command)
			validIndexes = append(validIndexes, index)
		}
	}
	if len(validCommands) == 0 {
		return results
	}
	metadataResults := gateway.repository.LoadAttemptMetadataBatch(ctx, validCommands)
	if len(metadataResults) != len(validCommands) {
		for _, index := range validIndexes {
			results[index].Err = fmt.Errorf("execution context metadata batch returned an invalid result count")
		}
		return results
	}
	for batchIndex, index := range validIndexes {
		value, err := metadataResults[batchIndex].Metadata, metadataResults[batchIndex].Err
		if err != nil {
			results[index].Err = err
			continue
		}
		metadata[index] = value
		snapshot, err := gateway.snapshots.Load(ctx, value.ProjectID+"\x00"+value.SnapshotID, func(loadCtx stdcontext.Context) (json.RawMessage, error) {
			return gateway.repository.LoadSnapshotDSL(loadCtx, value.ProjectID, value.SnapshotID)
		})
		if err != nil {
			results[index].Err = err
			continue
		}
		node, err := snapshot.node(value)
		if err != nil {
			results[index].Err = err
			continue
		}
		snapshots[index] = snapshot
		key := value.ProjectID + "\x00" + value.RunID
		group := groups[key]
		if group == nil {
			group = &runGroup{projectID: value.ProjectID, runID: value.RunID, outputs: make(map[string]struct{})}
			groups[key] = group
		}
		group.items = append(group.items, index)
		for _, binding := range node.Inputs {
			if binding.Kind != dsl.BindingNodeOutput || binding.Output == nil {
				continue
			}
			upstreamID := string(binding.Output.NodeID)
			if upstreamID != string(snapshot.entryNodeID) || binding.Output.Name != "workflow_input" {
				group.outputs[upstreamID] = struct{}{}
			}
		}
	}
	for _, group := range groups {
		ids := make([]string, 0, len(group.outputs))
		for id := range group.outputs {
			ids = append(ids, id)
		}
		group.input, group.values, group.err = gateway.repository.LoadRunContext(ctx, group.projectID, group.runID, ids)
	}
	for _, group := range groups {
		for _, index := range group.items {
			if group.err != nil {
				results[index].Err = group.err
				continue
			}
			results[index].Context, results[index].Err = assemble(commands[index], metadata[index], snapshots[index], group.input, group.values)
		}
	}
	return results
}

func validateLoadCommand(command LoadCommand) error {
	if command.ProjectID == "" || command.RunID == "" || command.AttemptID == "" || command.AttemptSequence == 0 || command.LeaseToken == "" || command.FencingToken == 0 {
		return fmt.Errorf("execution context attempt coordinate and lease are required")
	}
	return nil
}

func assemble(command LoadCommand, metadata Metadata, snapshot *parsedSnapshot, workflowInput json.RawMessage, outputs map[string]json.RawMessage) (ExecutionContext, error) {
	if !json.Valid(workflowInput) {
		return ExecutionContext{}, fmt.Errorf("authoritative workflow input is not valid JSON")
	}
	node, err := snapshot.node(metadata)
	if err != nil {
		return ExecutionContext{}, err
	}
	upstream := make(map[string]json.RawMessage)
	for _, binding := range node.Inputs {
		if binding.Kind != dsl.BindingNodeOutput || binding.Output == nil {
			continue
		}
		upstreamID := string(binding.Output.NodeID)
		if _, exists := upstream[upstreamID]; exists {
			continue
		}
		if upstreamID == string(snapshot.entryNodeID) && binding.Output.Name == "workflow_input" {
			upstream[upstreamID] = cloneRaw(workflowInput)
			continue
		}
		value, exists := outputs[upstreamID]
		if !exists || !json.Valid(value) {
			return ExecutionContext{}, fmt.Errorf("authoritative output for node %q is unavailable or invalid", upstreamID)
		}
		upstream[upstreamID] = cloneRaw(value)
	}
	return ExecutionContext{
		ContextVersion: 1, ProjectID: metadata.ProjectID, RunID: metadata.RunID,
		NodeRunID: metadata.NodeRunID, ExecutionNodeID: metadata.ExecutionNodeID,
		AttemptID: metadata.AttemptID, AttemptSequence: metadata.AttemptSequence,
		LeaseToken: command.LeaseToken, FencingToken: command.FencingToken,
		SnapshotID: metadata.SnapshotID, ResourceClass: metadata.ResourceClass,
		Operation: cloneOperation(node.Operation), ExecutionPolicy: clonePolicy(node.ExecutionPolicy), OutputContract: cloneOutputs(node.Outputs),
		WorkflowInput: cloneRaw(workflowInput), Inputs: cloneMap(metadata.ResolvedInputs),
		UpstreamOutputs: upstream, ResourceMaterial: map[string]json.RawMessage{},
	}, nil
}

func executionNode(snapshotJSON json.RawMessage, metadata Metadata) (*dsl.Node, error) {
	snapshot, err := parseSnapshot(snapshotJSON)
	if err != nil {
		return nil, err
	}
	return snapshot.node(metadata)
}

func cloneRaw(value json.RawMessage) json.RawMessage { return append(json.RawMessage(nil), value...) }

func cloneMap(values map[string]json.RawMessage) map[string]json.RawMessage {
	result := make(map[string]json.RawMessage, len(values))
	for key, value := range values {
		result[key] = cloneRaw(value)
	}
	return result
}

func cloneOperation(value dsl.Operation) dsl.Operation {
	value.Config = cloneMap(value.Config)
	return value
}

func cloneOutputs(values map[dsl.PortName]dsl.DataType) map[dsl.PortName]dsl.DataType {
	result := make(map[dsl.PortName]dsl.DataType, len(values))
	for key, value := range values {
		result[key] = value
	}
	return result
}

func clonePolicy(value dsl.ExecutionPolicy) dsl.ExecutionPolicy {
	value.RetryableErrorCodes = append([]string(nil), value.RetryableErrorCodes...)
	if value.RetryBackoff != nil {
		backoff := *value.RetryBackoff
		value.RetryBackoff = &backoff
	}
	return value
}
