package cli

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/uu999/evalfrog/internal/ir"
)

type builderEnvelope struct {
	OK    bool `json:"ok"`
	Data  any  `json:"data,omitempty"`
	Error *struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error,omitempty"`
}

func (app App) builder(ctx context.Context, arguments []string) int {
	if len(arguments) == 0 {
		return app.builderFailure("USAGE", "usage: evalfrog workflow builder <init|pull|create|copy|add-node|remove-node|add-edge|remove-edge|set-title|set-input|bind|remove-input|set-output|remove-output|set-layout|check|preview|push|validate> ...", 2)
	}
	switch arguments[0] {
	case "init":
		return app.builderInit(arguments[1:])
	case "pull":
		return app.builderPull(ctx, arguments[1:])
	case "create":
		return app.builderCreate(ctx, arguments[1:])
	case "copy":
		return app.builderCopy(ctx, arguments[1:])
	case "add-node":
		return app.builderAddNode(arguments[1:])
	case "remove-node":
		return app.builderRemoveNode(arguments[1:])
	case "add-edge":
		return app.builderAddEdge(arguments[1:])
	case "remove-edge":
		return app.builderRemoveEdge(arguments[1:])
	case "set-title":
		return app.builderSetTitle(arguments[1:])
	case "set-input":
		return app.builderSetInput(arguments[1:])
	case "bind":
		return app.builderBind(arguments[1:])
	case "remove-input":
		return app.builderRemoveInput(arguments[1:])
	case "set-output":
		return app.builderSetOutput(arguments[1:])
	case "remove-output":
		return app.builderRemoveOutput(arguments[1:])
	case "set-layout":
		return app.builderSetLayout(arguments[1:])
	case "check":
		return app.builderCheck(arguments[1:])
	case "preview":
		return app.builderPreview(arguments[1:])
	case "push":
		return app.builderPush(ctx, arguments[1:])
	case "validate":
		return app.builderValidate(ctx, arguments[1:])
	default:
		return app.builderFailure("USAGE", "unknown builder command "+arguments[0], 2)
	}
}

func builderFlagSet(name string) *flag.FlagSet {
	flags := flag.NewFlagSet("workflow builder "+name, flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	return flags
}

func (app App) builderSuccess(data any) int {
	_ = json.NewEncoder(app.Output).Encode(builderEnvelope{OK: true, Data: data})
	return 0
}

func (app App) builderFailure(code, message string, exitCode int) int {
	envelope := builderEnvelope{OK: false, Error: &struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	}{Code: code, Message: message}}
	_ = json.NewEncoder(app.Output).Encode(envelope)
	return exitCode
}

func (app App) builderFailureFor(err error) int {
	if api, ok := err.(*apiError); ok && api.Code != "" {
		message := api.Message
		if api.Code == "DRAFT_REVISION_CONFLICT" {
			message = "draft revision conflict: pull the latest Draft before retrying"
		}
		return app.builderFailure(api.Code, message, 1)
	}
	return app.builderFailure("BUILDER_ERROR", err.Error(), 1)
}

func builderSessionData(session builderSession) map[string]any {
	return map[string]any{
		"session":     session.Directory,
		"workflow_id": session.Meta.WorkflowID,
		"revision":    session.Meta.Revision,
		"dirty":       session.dirty(),
	}
}

func (app App) builderInit(arguments []string) int {
	flags := builderFlagSet("init")
	sessionDirectory := flags.String("session", "", "local builder session directory")
	fromIR := flags.String("from-ir", "", "initial IR JSON file")
	if flags.Parse(arguments) != nil || *sessionDirectory == "" {
		return app.builderFailure("INVALID_ARGUMENT", "--session is required", 2)
	}
	var session builderSession
	var err error
	if *fromIR == "" {
		session, err = newEmptyBuilderSession(*sessionDirectory)
	} else {
		raw, readErr := os.ReadFile(*fromIR)
		if readErr != nil {
			return app.builderFailureFor(readErr)
		}
		document, diagnostics := ir.DefaultParser().ParseDraft(raw)
		if ir.HasErrors(diagnostics) {
			encoded, _ := json.Marshal(diagnostics)
			return app.builderFailure("LOCAL_IR_INVALID", string(encoded), 1)
		}
		session, err = newBuilderSession(*sessionDirectory, document)
	}
	if err != nil {
		return app.builderFailureFor(err)
	}
	if err = initializeBuilderSession(&session); err != nil {
		return app.builderFailureFor(err)
	}
	return app.builderSuccess(builderSessionData(session))
}

func (app App) builderPull(ctx context.Context, arguments []string) int {
	flags := builderFlagSet("pull")
	sessionDirectory := flags.String("session", "", "local builder session directory")
	discardLocal := flags.Bool("discard-local", false, "replace local builder changes")
	common := app.common(flags, true)
	workflowID := flags.String("workflow", "", "workflow UUID")
	if flags.Parse(arguments) != nil || *sessionDirectory == "" || *workflowID == "" {
		return app.builderFailure("INVALID_ARGUMENT", "--session, --workflow, --server, --token and --project are required", 2)
	}
	client, err := builderNewClient(common.server, common.token, common.project, app.HTTP)
	if err != nil {
		return app.builderFailureFor(err)
	}
	session, err := loadBuilderSession(*sessionDirectory)
	if err != nil {
		if !errors.Is(err, errBuilderSessionNotFound) {
			return app.builderFailureFor(err)
		}
		session, err = newEmptyBuilderSession(*sessionDirectory)
		if err != nil {
			return app.builderFailureFor(err)
		}
	} else if session.dirty() && !*discardLocal {
		return app.builderFailure("LOCAL_CHANGES_NOT_PUSHED", "builder session has local changes; pass --discard-local to replace them", 1)
	}
	var response apiDraftResponse
	err = client.request(ctx, http.MethodGet, "/v1/projects/"+common.project+"/workflows/"+*workflowID+"/draft", "", nil, &response)
	if err != nil {
		return app.builderFailureFor(err)
	}
	document, diagnostics := ir.DefaultParser().ParseDraft(response.Current.IR)
	if ir.HasErrors(diagnostics) {
		encoded, _ := json.Marshal(diagnostics)
		return app.builderFailure("REMOTE_IR_INVALID", string(encoded), 1)
	}
	if err = session.markSynchronized(common.server, common.project, *workflowID, response.Current.Revision, document); err != nil {
		return app.builderFailureFor(err)
	}
	if err = saveBuilderSession(&session); err != nil {
		return app.builderFailureFor(err)
	}
	if err = saveWorkspace(app.workspaceRoot(), Workspace{Server: common.server, ProjectID: common.project, WorkflowID: *workflowID, Revision: response.Current.Revision, IR: response.Current.IR}); err != nil {
		return app.builderFailureFor(err)
	}
	return app.builderSuccess(builderSessionData(session))
}

func (app App) builderCreate(ctx context.Context, arguments []string) int {
	flags := builderFlagSet("create")
	sessionDirectory := flags.String("session", "", "local builder session directory")
	name := flags.String("name", "", "workflow name")
	key := flags.String("idempotency-key", "", "idempotency key")
	common := app.common(flags, true)
	if flags.Parse(arguments) != nil || *sessionDirectory == "" || *name == "" || *key == "" {
		return app.builderFailure("INVALID_ARGUMENT", "--session, --name, --idempotency-key, --server, --token and --project are required", 2)
	}
	session, err := loadBuilderSession(*sessionDirectory)
	if err != nil {
		return app.builderFailureFor(err)
	}
	if session.Meta.WorkflowID != "" {
		return app.builderFailure("SESSION_ALREADY_BOUND", "builder session already belongs to a Workflow; use push or init a new session", 1)
	}
	client, err := builderNewClient(common.server, common.token, common.project, app.HTTP)
	if err != nil {
		return app.builderFailureFor(err)
	}
	canonical, err := ir.CanonicalizeDocument(session.Document)
	if err != nil {
		return app.builderFailureFor(err)
	}
	var response struct {
		Workflow apiWorkflowResponse      `json:"workflow"`
		Draft    apiDraftRevisionResponse `json:"draft_revision"`
	}
	err = client.request(ctx, http.MethodPost, "/v1/projects/"+common.project+"/workflows", *key, map[string]any{"name": *name, "ir": json.RawMessage(canonical)}, &response)
	if err != nil {
		return app.builderFailureFor(err)
	}
	document, diagnostics := ir.DefaultParser().ParseDraft(response.Draft.IR)
	if ir.HasErrors(diagnostics) {
		encoded, _ := json.Marshal(diagnostics)
		return app.builderFailure("REMOTE_IR_INVALID", string(encoded), 1)
	}
	if err = session.markSynchronized(common.server, common.project, response.Workflow.ID, response.Draft.Revision, document); err != nil {
		return app.builderFailureFor(err)
	}
	if err = saveBuilderSession(&session); err != nil {
		return app.builderFailureFor(err)
	}
	if err = saveWorkspace(app.workspaceRoot(), Workspace{Server: common.server, ProjectID: common.project, WorkflowID: response.Workflow.ID, Revision: response.Draft.Revision, IR: response.Draft.IR}); err != nil {
		return app.builderFailureFor(err)
	}
	return app.builderSuccess(builderSessionData(session))
}

func (app App) builderCopy(ctx context.Context, arguments []string) int {
	flags := builderFlagSet("copy")
	sessionDirectory := flags.String("session", "", "local builder session directory")
	sourceWorkflow := flags.String("source-workflow", "", "source workflow UUID")
	version := flags.Int64("version", 0, "published version")
	name := flags.String("name", "", "new workflow name")
	key := flags.String("idempotency-key", "", "idempotency key")
	common := app.common(flags, true)
	if flags.Parse(arguments) != nil || *sessionDirectory == "" || *sourceWorkflow == "" || *version < 1 || *name == "" || *key == "" {
		return app.builderFailure("INVALID_ARGUMENT", "--session, --source-workflow, --version, --name, --idempotency-key, --server, --token and --project are required", 2)
	}
	session, err := loadBuilderSession(*sessionDirectory)
	if err != nil {
		if !errors.Is(err, errBuilderSessionNotFound) {
			return app.builderFailureFor(err)
		}
		session, err = newEmptyBuilderSession(*sessionDirectory)
		if err != nil {
			return app.builderFailureFor(err)
		}
	} else if session.Meta.WorkflowID != "" || session.dirty() {
		return app.builderFailure("SESSION_NOT_EMPTY", "copy requires a new or clean unbound builder session", 1)
	}
	client, err := builderNewClient(common.server, common.token, common.project, app.HTTP)
	if err != nil {
		return app.builderFailureFor(err)
	}
	var response struct {
		Workflow apiWorkflowResponse      `json:"workflow"`
		Draft    apiDraftRevisionResponse `json:"draft_revision"`
	}
	err = client.request(ctx, http.MethodPost, "/v1/projects/"+common.project+"/workflows:copy", *key, map[string]any{"source_workflow_id": *sourceWorkflow, "source_version_number": *version, "name": *name}, &response)
	if err != nil {
		return app.builderFailureFor(err)
	}
	document, diagnostics := ir.DefaultParser().ParseDraft(response.Draft.IR)
	if ir.HasErrors(diagnostics) {
		encoded, _ := json.Marshal(diagnostics)
		return app.builderFailure("REMOTE_IR_INVALID", string(encoded), 1)
	}
	if err = session.markSynchronized(common.server, common.project, response.Workflow.ID, response.Draft.Revision, document); err != nil {
		return app.builderFailureFor(err)
	}
	if err = saveBuilderSession(&session); err != nil {
		return app.builderFailureFor(err)
	}
	if err = saveWorkspace(app.workspaceRoot(), Workspace{Server: common.server, ProjectID: common.project, WorkflowID: response.Workflow.ID, Revision: response.Draft.Revision, IR: response.Draft.IR}); err != nil {
		return app.builderFailureFor(err)
	}
	return app.builderSuccess(builderSessionData(session))
}

func (app App) builderAddNode(arguments []string) int {
	flags := builderFlagSet("add-node")
	sessionDirectory := flags.String("session", "", "local builder session directory")
	id := flags.String("id", "", "semantic node ID")
	nodeType := flags.String("type", "", "node type")
	title := flags.String("title", "", "node title")
	if flags.Parse(arguments) != nil || *sessionDirectory == "" || *id == "" || *nodeType == "" || strings.TrimSpace(*title) == "" {
		return app.builderFailure("INVALID_ARGUMENT", "--session, --id, --type and --title are required", 2)
	}
	nodeID := ir.LogicalID(*id)
	if !ir.ValidLogicalID(nodeID) || !ir.ValidNodeType(ir.NodeType(*nodeType)) {
		return app.builderFailure("INVALID_ARGUMENT", "node id or type is invalid", 2)
	}
	session, err := loadBuilderSession(*sessionDirectory)
	if err != nil {
		return app.builderFailureFor(err)
	}
	if _, index := builderNode(session.Document, nodeID); index >= 0 {
		return app.builderFailure("NODE_ALREADY_EXISTS", "node id already exists", 1)
	}
	session.Document.Nodes = append(session.Document.Nodes, ir.Node{ID: nodeID, Type: ir.NodeType(*nodeType), Title: *title, Inputs: []ir.Input{}, Outputs: []ir.Output{}})
	session.Document.Layout[nodeID] = builderPosition(0, 0)
	if err = saveBuilderSession(&session); err != nil {
		return app.builderFailureFor(err)
	}
	return app.builderSuccess(builderSessionData(session))
}

func (app App) builderRemoveNode(arguments []string) int {
	flags := builderFlagSet("remove-node")
	sessionDirectory := flags.String("session", "", "local builder session directory")
	id := flags.String("id", "", "node ID")
	if flags.Parse(arguments) != nil || *sessionDirectory == "" || *id == "" {
		return app.builderFailure("INVALID_ARGUMENT", "--session and --id are required", 2)
	}
	session, err := loadBuilderSession(*sessionDirectory)
	if err != nil {
		return app.builderFailureFor(err)
	}
	nodeID := ir.LogicalID(*id)
	_, index := builderNode(session.Document, nodeID)
	if index < 0 {
		return app.builderFailure("NODE_NOT_FOUND", "node id does not exist", 1)
	}
	session.Document.Nodes = append(session.Document.Nodes[:index], session.Document.Nodes[index+1:]...)
	edges := session.Document.Edges[:0]
	for _, edge := range session.Document.Edges {
		if edge.Source != nodeID && edge.Target != nodeID {
			edges = append(edges, edge)
		}
	}
	session.Document.Edges = edges
	delete(session.Document.Layout, nodeID)
	if err = saveBuilderSession(&session); err != nil {
		return app.builderFailureFor(err)
	}
	return app.builderSuccess(builderSessionData(session))
}

func (app App) builderAddEdge(arguments []string) int {
	flags := builderFlagSet("add-edge")
	sessionDirectory := flags.String("session", "", "local builder session directory")
	id := flags.String("id", "", "semantic edge ID")
	source := flags.String("source", "", "source node ID")
	target := flags.String("target", "", "target node ID")
	route := flags.String("route", "", "branch route")
	if flags.Parse(arguments) != nil || *sessionDirectory == "" || *id == "" || *source == "" || *target == "" {
		return app.builderFailure("INVALID_ARGUMENT", "--session, --id, --source and --target are required", 2)
	}
	edgeID, sourceID, targetID := ir.LogicalID(*id), ir.LogicalID(*source), ir.LogicalID(*target)
	if !ir.ValidLogicalID(edgeID) || !ir.ValidLogicalID(sourceID) || !ir.ValidLogicalID(targetID) || (*route != "" && !ir.ValidRouteName(ir.RouteName(*route))) {
		return app.builderFailure("INVALID_ARGUMENT", "edge id, node ids or route are invalid", 2)
	}
	session, err := loadBuilderSession(*sessionDirectory)
	if err != nil {
		return app.builderFailureFor(err)
	}
	if _, index := builderNode(session.Document, sourceID); index < 0 {
		return app.builderFailure("SOURCE_NODE_NOT_FOUND", "edge source node does not exist", 1)
	}
	if _, index := builderNode(session.Document, targetID); index < 0 {
		return app.builderFailure("TARGET_NODE_NOT_FOUND", "edge target node does not exist", 1)
	}
	for _, edge := range session.Document.Edges {
		if edge.ID == edgeID {
			return app.builderFailure("EDGE_ALREADY_EXISTS", "edge id already exists", 1)
		}
	}
	session.Document.Edges = append(session.Document.Edges, ir.Edge{ID: edgeID, Source: sourceID, Target: targetID, Route: ir.RouteName(*route)})
	if err = saveBuilderSession(&session); err != nil {
		return app.builderFailureFor(err)
	}
	return app.builderSuccess(builderSessionData(session))
}

func (app App) builderRemoveEdge(arguments []string) int {
	flags := builderFlagSet("remove-edge")
	sessionDirectory := flags.String("session", "", "local builder session directory")
	id := flags.String("id", "", "edge ID")
	if flags.Parse(arguments) != nil || *sessionDirectory == "" || *id == "" {
		return app.builderFailure("INVALID_ARGUMENT", "--session and --id are required", 2)
	}
	session, err := loadBuilderSession(*sessionDirectory)
	if err != nil {
		return app.builderFailureFor(err)
	}
	edges := session.Document.Edges[:0]
	found := false
	for _, edge := range session.Document.Edges {
		if edge.ID == ir.LogicalID(*id) {
			found = true
			continue
		}
		edges = append(edges, edge)
	}
	if !found {
		return app.builderFailure("EDGE_NOT_FOUND", "edge id does not exist", 1)
	}
	session.Document.Edges = edges
	if err = saveBuilderSession(&session); err != nil {
		return app.builderFailureFor(err)
	}
	return app.builderSuccess(builderSessionData(session))
}

func (app App) builderSetTitle(arguments []string) int {
	flags := builderFlagSet("set-title")
	sessionDirectory := flags.String("session", "", "local builder session directory")
	node := flags.String("node", "", "node ID")
	title := flags.String("title", "", "node title")
	if flags.Parse(arguments) != nil || *sessionDirectory == "" || *node == "" || strings.TrimSpace(*title) == "" {
		return app.builderFailure("INVALID_ARGUMENT", "--session, --node and --title are required", 2)
	}
	session, err := loadBuilderSession(*sessionDirectory)
	if err != nil {
		return app.builderFailureFor(err)
	}
	_, index := builderNode(session.Document, ir.LogicalID(*node))
	if index < 0 {
		return app.builderFailure("NODE_NOT_FOUND", "node id does not exist", 1)
	}
	session.Document.Nodes[index].Title = *title
	if err = saveBuilderSession(&session); err != nil {
		return app.builderFailureFor(err)
	}
	return app.builderSuccess(builderSessionData(session))
}

func (app App) builderSetInput(arguments []string) int {
	flags := builderFlagSet("set-input")
	sessionDirectory := flags.String("session", "", "local builder session directory")
	node := flags.String("node", "", "node ID")
	name := flags.String("name", "", "input name")
	dataType := flags.String("data-type", "", "input data type")
	literal := flags.String("literal", "", "literal JSON value")
	literalFile := flags.String("literal-file", "", "file containing literal JSON value")
	if flags.Parse(arguments) != nil || *sessionDirectory == "" || *node == "" || *name == "" || *dataType == "" {
		return app.builderFailure("INVALID_ARGUMENT", "--session, --node, --name and --data-type are required", 2)
	}
	if (*literal == "" && *literalFile == "") || (*literal != "" && *literalFile != "") {
		return app.builderFailure("INVALID_ARGUMENT", "set-input requires exactly one of --literal or --literal-file", 2)
	}
	typeValue, err := builderDataType(*dataType)
	if err != nil {
		return app.builderFailureFor(err)
	}
	raw, err := builderLiteral(*literal, *literalFile)
	if err != nil {
		return app.builderFailureFor(err)
	}
	return app.builderUpsertInput(*sessionDirectory, ir.LogicalID(*node), ir.Input{Name: ir.PortName(*name), DataType: typeValue, Source: ir.SourceLiteral, Value: raw})
}

func (app App) builderBind(arguments []string) int {
	flags := builderFlagSet("bind")
	sessionDirectory := flags.String("session", "", "local builder session directory")
	node := flags.String("node", "", "target node ID")
	name := flags.String("name", "", "target input name")
	dataType := flags.String("data-type", "", "input data type")
	sourceNode := flags.String("source-node", "", "source node ID")
	sourceOutput := flags.String("source-output", "", "source output name")
	if flags.Parse(arguments) != nil || *sessionDirectory == "" || *node == "" || *name == "" || *dataType == "" || *sourceNode == "" || *sourceOutput == "" {
		return app.builderFailure("INVALID_ARGUMENT", "--session, --node, --name, --data-type, --source-node and --source-output are required", 2)
	}
	typeValue, err := builderDataType(*dataType)
	if err != nil {
		return app.builderFailureFor(err)
	}
	return app.builderUpsertInput(*sessionDirectory, ir.LogicalID(*node), ir.Input{Name: ir.PortName(*name), DataType: typeValue, Source: ir.SourceRef, RefNode: ir.LogicalID(*sourceNode), RefOutput: ir.PortName(*sourceOutput)})
}

func (app App) builderUpsertInput(sessionDirectory string, nodeID ir.LogicalID, input ir.Input) int {
	if !ir.ValidLogicalID(nodeID) || !ir.ValidPortName(input.Name) {
		return app.builderFailure("INVALID_ARGUMENT", "node id or input name is invalid", 2)
	}
	if input.Source == ir.SourceRef && (!ir.ValidLogicalID(input.RefNode) || !ir.ValidPortName(input.RefOutput)) {
		return app.builderFailure("INVALID_ARGUMENT", "reference node id or output name is invalid", 2)
	}
	session, err := loadBuilderSession(sessionDirectory)
	if err != nil {
		return app.builderFailureFor(err)
	}
	_, index := builderNode(session.Document, nodeID)
	if index < 0 {
		return app.builderFailure("NODE_NOT_FOUND", "node id does not exist", 1)
	}
	if input.Source == ir.SourceRef {
		source, sourceIndex := builderNode(session.Document, input.RefNode)
		if sourceIndex < 0 {
			return app.builderFailure("SOURCE_NODE_NOT_FOUND", "reference source node does not exist", 1)
		}
		outputExists := false
		for _, output := range source.Outputs {
			if output.Name == input.RefOutput {
				outputExists = true
				break
			}
		}
		if !outputExists {
			return app.builderFailure("SOURCE_OUTPUT_NOT_FOUND", "reference source output does not exist", 1)
		}
	}
	found := false
	for inputIndex := range session.Document.Nodes[index].Inputs {
		if session.Document.Nodes[index].Inputs[inputIndex].Name == input.Name {
			session.Document.Nodes[index].Inputs[inputIndex] = input
			found = true
			break
		}
	}
	if !found {
		session.Document.Nodes[index].Inputs = append(session.Document.Nodes[index].Inputs, input)
	}
	if err = saveBuilderSession(&session); err != nil {
		return app.builderFailureFor(err)
	}
	return app.builderSuccess(builderSessionData(session))
}

func (app App) builderRemoveInput(arguments []string) int {
	flags := builderFlagSet("remove-input")
	sessionDirectory := flags.String("session", "", "local builder session directory")
	node := flags.String("node", "", "node ID")
	name := flags.String("name", "", "input name")
	if flags.Parse(arguments) != nil || *sessionDirectory == "" || *node == "" || *name == "" {
		return app.builderFailure("INVALID_ARGUMENT", "--session, --node and --name are required", 2)
	}
	session, err := loadBuilderSession(*sessionDirectory)
	if err != nil {
		return app.builderFailureFor(err)
	}
	_, index := builderNode(session.Document, ir.LogicalID(*node))
	if index < 0 {
		return app.builderFailure("NODE_NOT_FOUND", "node id does not exist", 1)
	}
	inputs, found := builderRemoveInput(session.Document.Nodes[index].Inputs, ir.PortName(*name))
	if !found {
		return app.builderFailure("INPUT_NOT_FOUND", "input name does not exist", 1)
	}
	session.Document.Nodes[index].Inputs = inputs
	if err = saveBuilderSession(&session); err != nil {
		return app.builderFailureFor(err)
	}
	return app.builderSuccess(builderSessionData(session))
}

func (app App) builderSetOutput(arguments []string) int {
	flags := builderFlagSet("set-output")
	sessionDirectory := flags.String("session", "", "local builder session directory")
	node := flags.String("node", "", "node ID")
	name := flags.String("name", "", "output name")
	dataType := flags.String("data-type", "", "output data type")
	if flags.Parse(arguments) != nil || *sessionDirectory == "" || *node == "" || *name == "" || *dataType == "" {
		return app.builderFailure("INVALID_ARGUMENT", "--session, --node, --name and --data-type are required", 2)
	}
	if !ir.ValidPortName(ir.PortName(*name)) {
		return app.builderFailure("INVALID_ARGUMENT", "output name is invalid", 2)
	}
	typeValue, err := builderDataType(*dataType)
	if err != nil {
		return app.builderFailureFor(err)
	}
	session, err := loadBuilderSession(*sessionDirectory)
	if err != nil {
		return app.builderFailureFor(err)
	}
	_, index := builderNode(session.Document, ir.LogicalID(*node))
	if index < 0 {
		return app.builderFailure("NODE_NOT_FOUND", "node id does not exist", 1)
	}
	found := false
	for outputIndex := range session.Document.Nodes[index].Outputs {
		if session.Document.Nodes[index].Outputs[outputIndex].Name == ir.PortName(*name) {
			session.Document.Nodes[index].Outputs[outputIndex].DataType = typeValue
			found = true
			break
		}
	}
	if !found {
		session.Document.Nodes[index].Outputs = append(session.Document.Nodes[index].Outputs, ir.Output{Name: ir.PortName(*name), DataType: typeValue})
	}
	if err = saveBuilderSession(&session); err != nil {
		return app.builderFailureFor(err)
	}
	return app.builderSuccess(builderSessionData(session))
}

func (app App) builderRemoveOutput(arguments []string) int {
	flags := builderFlagSet("remove-output")
	sessionDirectory := flags.String("session", "", "local builder session directory")
	node := flags.String("node", "", "node ID")
	name := flags.String("name", "", "output name")
	if flags.Parse(arguments) != nil || *sessionDirectory == "" || *node == "" || *name == "" {
		return app.builderFailure("INVALID_ARGUMENT", "--session, --node and --name are required", 2)
	}
	session, err := loadBuilderSession(*sessionDirectory)
	if err != nil {
		return app.builderFailureFor(err)
	}
	_, index := builderNode(session.Document, ir.LogicalID(*node))
	if index < 0 {
		return app.builderFailure("NODE_NOT_FOUND", "node id does not exist", 1)
	}
	outputs := session.Document.Nodes[index].Outputs[:0]
	found := false
	for _, output := range session.Document.Nodes[index].Outputs {
		if output.Name == ir.PortName(*name) {
			found = true
			continue
		}
		outputs = append(outputs, output)
	}
	if !found {
		return app.builderFailure("OUTPUT_NOT_FOUND", "output name does not exist", 1)
	}
	session.Document.Nodes[index].Outputs = outputs
	if err = saveBuilderSession(&session); err != nil {
		return app.builderFailureFor(err)
	}
	return app.builderSuccess(builderSessionData(session))
}

func (app App) builderSetLayout(arguments []string) int {
	flags := builderFlagSet("set-layout")
	sessionDirectory := flags.String("session", "", "local builder session directory")
	node := flags.String("node", "", "node ID")
	x := flags.Float64("x", 0, "x coordinate")
	y := flags.Float64("y", 0, "y coordinate")
	if flags.Parse(arguments) != nil || *sessionDirectory == "" || *node == "" {
		return app.builderFailure("INVALID_ARGUMENT", "--session and --node are required", 2)
	}
	session, err := loadBuilderSession(*sessionDirectory)
	if err != nil {
		return app.builderFailureFor(err)
	}
	nodeID := ir.LogicalID(*node)
	if _, index := builderNode(session.Document, nodeID); index < 0 {
		return app.builderFailure("NODE_NOT_FOUND", "node id does not exist", 1)
	}
	session.Document.Layout[nodeID] = builderPosition(*x, *y)
	if err = saveBuilderSession(&session); err != nil {
		return app.builderFailureFor(err)
	}
	return app.builderSuccess(builderSessionData(session))
}

func (app App) builderCheck(arguments []string) int {
	flags := builderFlagSet("check")
	sessionDirectory := flags.String("session", "", "local builder session directory")
	if flags.Parse(arguments) != nil || *sessionDirectory == "" {
		return app.builderFailure("INVALID_ARGUMENT", "--session is required", 2)
	}
	session, err := loadBuilderSession(*sessionDirectory)
	if err != nil {
		return app.builderFailureFor(err)
	}
	diagnostics := ir.NewStructuralValidator().Validate(session.Document)
	return app.builderSuccess(map[string]any{"valid": !ir.HasErrors(diagnostics), "scope": "structural", "diagnostics": diagnostics, "dirty": session.dirty()})
}

func (app App) builderPreview(arguments []string) int {
	flags := builderFlagSet("preview")
	sessionDirectory := flags.String("session", "", "local builder session directory")
	outputPath := flags.String("out", "", "write canonical IR JSON to file")
	if flags.Parse(arguments) != nil || *sessionDirectory == "" {
		return app.builderFailure("INVALID_ARGUMENT", "--session is required", 2)
	}
	session, err := loadBuilderSession(*sessionDirectory)
	if err != nil {
		return app.builderFailureFor(err)
	}
	canonical, err := ir.CanonicalizeDocument(session.Document)
	if err != nil {
		return app.builderFailureFor(err)
	}
	if *outputPath != "" {
		if err = os.WriteFile(*outputPath, append(canonical, '\n'), 0o600); err != nil {
			return app.builderFailureFor(err)
		}
	}
	return app.builderSuccess(map[string]any{"session": session.Directory, "dirty": session.dirty(), "ir_hash": ir.HashCanonical(canonical), "ir": json.RawMessage(canonical), "out": *outputPath})
}

func (app App) builderPush(ctx context.Context, arguments []string) int {
	flags := builderFlagSet("push")
	sessionDirectory := flags.String("session", "", "local builder session directory")
	token := flags.String("token", "", "bearer API token")
	key := flags.String("idempotency-key", "", "idempotency key")
	if flags.Parse(arguments) != nil || *sessionDirectory == "" || *token == "" || *key == "" {
		return app.builderFailure("INVALID_ARGUMENT", "--session, --token and --idempotency-key are required", 2)
	}
	session, err := loadBuilderSession(*sessionDirectory)
	if err != nil {
		return app.builderFailureFor(err)
	}
	if session.Meta.WorkflowID == "" {
		return app.builderFailure("WORKFLOW_REQUIRED", "builder session is not bound; use builder create or copy first", 1)
	}
	if !session.dirty() {
		return app.builderFailure("NO_LOCAL_CHANGES", "builder session has no local changes to push", 1)
	}
	client, err := builderNewClient(session.Meta.Server, *token, session.Meta.ProjectID, app.HTTP)
	if err != nil {
		return app.builderFailureFor(err)
	}
	canonical, err := ir.CanonicalizeDocument(session.Document)
	if err != nil {
		return app.builderFailureFor(err)
	}
	var response apiDraftRevisionResponse
	err = client.request(ctx, http.MethodPut, "/v1/projects/"+session.Meta.ProjectID+"/workflows/"+session.Meta.WorkflowID+"/draft", *key, map[string]any{"expected_revision": session.Meta.Revision, "ir": json.RawMessage(canonical)}, &response)
	if err != nil {
		return app.builderFailureFor(err)
	}
	document, diagnostics := ir.DefaultParser().ParseDraft(response.IR)
	if ir.HasErrors(diagnostics) {
		encoded, _ := json.Marshal(diagnostics)
		return app.builderFailure("REMOTE_IR_INVALID", string(encoded), 1)
	}
	if err = session.markSynchronized(session.Meta.Server, session.Meta.ProjectID, session.Meta.WorkflowID, response.Revision, document); err != nil {
		return app.builderFailureFor(err)
	}
	if err = saveBuilderSession(&session); err != nil {
		return app.builderFailureFor(err)
	}
	if err = saveWorkspace(app.workspaceRoot(), Workspace{Server: session.Meta.Server, ProjectID: session.Meta.ProjectID, WorkflowID: session.Meta.WorkflowID, Revision: response.Revision, IR: response.IR}); err != nil {
		return app.builderFailureFor(err)
	}
	return app.builderSuccess(builderSessionData(session))
}

func (app App) builderValidate(ctx context.Context, arguments []string) int {
	flags := builderFlagSet("validate")
	sessionDirectory := flags.String("session", "", "local builder session directory")
	token := flags.String("token", "", "bearer API token")
	if flags.Parse(arguments) != nil || *sessionDirectory == "" || *token == "" {
		return app.builderFailure("INVALID_ARGUMENT", "--session and --token are required", 2)
	}
	session, err := loadBuilderSession(*sessionDirectory)
	if err != nil {
		return app.builderFailureFor(err)
	}
	if session.Meta.WorkflowID == "" {
		return app.builderFailure("WORKFLOW_REQUIRED", "builder session is not bound; use builder create or copy first", 1)
	}
	if session.dirty() {
		return app.builderFailure("LOCAL_CHANGES_NOT_PUSHED", "push local changes before server validation", 1)
	}
	client, err := builderNewClient(session.Meta.Server, *token, session.Meta.ProjectID, app.HTTP)
	if err != nil {
		return app.builderFailureFor(err)
	}
	var response struct {
		Valid       bool  `json:"valid"`
		Diagnostics []any `json:"diagnostics"`
	}
	err = client.request(ctx, http.MethodPost, "/v1/projects/"+session.Meta.ProjectID+"/workflows/"+session.Meta.WorkflowID+"/draft/validate", "", map[string]any{"revision": session.Meta.Revision}, &response)
	if err != nil {
		return app.builderFailureFor(err)
	}
	data := map[string]any{"valid": response.Valid, "scope": "server", "diagnostics": response.Diagnostics, "workflow_id": session.Meta.WorkflowID, "revision": session.Meta.Revision}
	if !response.Valid {
		_ = json.NewEncoder(app.Output).Encode(builderEnvelope{OK: true, Data: data})
		return 1
	}
	return app.builderSuccess(data)
}

func builderNewClient(server, token, projectID string, httpClient *http.Client) (*apiClient, error) {
	if strings.TrimSpace(projectID) == "" {
		return nil, errors.New("--project is required")
	}
	return newAPIClient(server, token, httpClient)
}

func builderNode(document ir.Document, id ir.LogicalID) (ir.Node, int) {
	for index, node := range document.Nodes {
		if node.ID == id {
			return node, index
		}
	}
	return ir.Node{}, -1
}

func builderRemoveInput(inputs []ir.Input, name ir.PortName) ([]ir.Input, bool) {
	result := inputs[:0]
	found := false
	for _, input := range inputs {
		if input.Name == name {
			found = true
			continue
		}
		result = append(result, input)
	}
	return result, found
}

func builderDataType(value string) (ir.DataType, error) {
	typeValue := ir.DataType(value)
	if !typeValue.Valid() {
		return "", fmt.Errorf("unsupported data type %q", value)
	}
	return typeValue, nil
}

func builderLiteral(literal, literalFile string) (json.RawMessage, error) {
	var raw []byte
	var err error
	if literalFile != "" {
		raw, err = os.ReadFile(literalFile)
		if err != nil {
			return nil, err
		}
	} else {
		raw = []byte(literal)
	}
	canonical, err := ir.CanonicalizeJSON(raw, ir.DefaultParseLimits)
	if err != nil {
		return nil, fmt.Errorf("literal JSON is invalid: %w", err)
	}
	if _, _, err = ir.DecodeLiteral(canonical); err != nil {
		return nil, err
	}
	return json.RawMessage(canonical), nil
}

func builderPosition(x, y float64) ir.Position {
	xValue := json.Number(strconv.FormatFloat(x, 'f', -1, 64))
	yValue := json.Number(strconv.FormatFloat(y, 'f', -1, 64))
	return ir.Position{X: &xValue, Y: &yValue}
}
