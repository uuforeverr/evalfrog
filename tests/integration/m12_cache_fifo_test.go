//go:build integration

package integration

import (
	"encoding/json"
	"reflect"
	"sort"
	"testing"
	"time"
)

func TestM12OutboxClaimsUseOccurredAtFIFOAndSkipBackoffHead(t *testing.T) {
	harness := newM5Harness(t)
	workflow, snapshot := harness.createCodeWorkflow(t, false)
	type row struct{ taskID, eventID string }
	rows := make([]row, 4)
	for index := range rows {
		run := harness.createTestRun(t, workflow.ID, snapshot.ID, "m12-fifo-run-"+newID(t))
		event := harness.initializeRun(t, run)
		nodeID := readyNodeID(t, harness, run.ID)
		task := dispatchFixture(t, harness, run.ID, nodeID)
		rows[index] = row{taskID: task.ID, eventID: event.EventID}
	}

	now := time.Now().UTC()
	base := now.Add(-time.Hour)
	occurred := []time.Time{base, base.Add(time.Second), base.Add(2 * time.Second), base.Add(2 * time.Second)}
	available := []time.Time{now.Add(time.Hour), now.Add(-time.Minute), now.Add(-10 * time.Minute), now.Add(-5 * time.Minute)}
	for index, value := range rows {
		mustExec(t, harness.ctx, harness.client.Pool(), `UPDATE node_task_outbox SET occurred_at=$1, available_at=$2 WHERE task_id=UUID_TO_BIN($3)`, occurred[index], available[index], value.taskID)
		mustExec(t, harness.ctx, harness.client.Pool(), `UPDATE outbox_events SET occurred_at=$1, available_at=$2 WHERE event_id=UUID_TO_BIN($3)`, occurred[index], available[index], value.eventID)
	}

	claimedTasks, err := harness.store.ClaimTaskOutbox(harness.ctx, "m12-task-relay", 10, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	gotTasks := make([]string, len(claimedTasks))
	for index, claimed := range claimedTasks {
		gotTasks[index] = claimed.Message.TaskID
	}
	wantTasks := stableFIFOIDs(rows[1].taskID, rows[2].taskID, rows[3].taskID)
	if !reflect.DeepEqual(gotTasks, wantTasks) {
		t.Fatalf("task claim order=%v want=%v", gotTasks, wantTasks)
	}
	for _, claimed := range claimedTasks {
		if claimed.Message.TaskID == rows[0].taskID {
			t.Fatal("backoff task was claimed before available_at")
		}
	}

	claimedEvents, err := harness.store.ClaimOutbox(harness.ctx, "m12-runtime-relay", 10, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	gotEvents := make([]string, len(claimedEvents))
	for index, claimed := range claimedEvents {
		gotEvents[index] = claimed.Event.EventID
	}
	wantEvents := stableFIFOIDs(rows[1].eventID, rows[2].eventID, rows[3].eventID)
	if !reflect.DeepEqual(gotEvents, wantEvents) {
		t.Fatalf("runtime event claim order=%v want=%v", gotEvents, wantEvents)
	}
}

func stableFIFOIDs(oldest string, tied ...string) []string {
	sort.Strings(tied)
	return append([]string{oldest}, tied...)
}

func jsonEqual(left, right json.RawMessage) bool {
	var leftValue, rightValue any
	return json.Unmarshal(left, &leftValue) == nil && json.Unmarshal(right, &rightValue) == nil && reflect.DeepEqual(leftValue, rightValue)
}
