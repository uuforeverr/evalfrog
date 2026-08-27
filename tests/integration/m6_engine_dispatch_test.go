//go:build integration

package integration

import (
	"testing"

	"github.com/uu999/evalfrog/internal/runtime"
)

func TestM6EngineInitializationAtomicallyQueuesAttemptAndTaskOutbox(t *testing.T) {
	harness := newM5Harness(t)
	workflow, snapshot := harness.createCodeWorkflow(t, false)
	run := harness.createTestRun(t, workflow.ID, snapshot.ID, "m6-engine-direct-dispatch")
	harness.initializeRun(t, run)

	var nodeState runtime.NodeState
	var attemptID, taskID string
	var attemptSequence uint32
	err := harness.client.Pool().QueryRow(harness.ctx, `
		SELECT n.state, a.attempt_id::text, o.task_id::text, a.attempt_seq
		FROM node_runs n
		JOIN node_attempts a
		  ON a.project_id=n.project_id AND a.run_id=n.run_id
		 AND a.attempt_id=n.current_attempt_id
		JOIN node_task_outbox o
		  ON o.project_id=a.project_id AND o.run_id=a.run_id
		 AND o.attempt_id=a.attempt_id
		WHERE n.project_id=$1 AND n.run_id=$2 AND n.kind='task'`,
		harness.projectID, run.ID).Scan(&nodeState, &attemptID, &taskID, &attemptSequence)
	if err != nil {
		t.Fatal(err)
	}
	if nodeState != runtime.NodeQueued || attemptID == "" || taskID != attemptID || attemptSequence != 1 {
		t.Fatalf("state=%s attempt=%s task=%s sequence=%d", nodeState, attemptID, taskID, attemptSequence)
	}
	var readyCount, attemptCount, taskCount int
	if err = harness.client.Pool().QueryRow(harness.ctx, `
		SELECT
		  count(*) FILTER (WHERE n.state='ready'),
		  count(DISTINCT a.attempt_id),
		  count(DISTINCT o.task_id)
		FROM node_runs n
		LEFT JOIN node_attempts a ON a.project_id=n.project_id AND a.run_id=n.run_id
		LEFT JOIN node_task_outbox o ON o.project_id=n.project_id AND o.run_id=n.run_id
		WHERE n.project_id=$1 AND n.run_id=$2`, harness.projectID, run.ID).
		Scan(&readyCount, &attemptCount, &taskCount); err != nil {
		t.Fatal(err)
	}
	if readyCount != 0 || attemptCount != 1 || taskCount != 1 {
		t.Fatalf("ready=%d attempts=%d tasks=%d", readyCount, attemptCount, taskCount)
	}
}

func TestM6DirectDispatchMigrationRemovesSchedulerIndexes(t *testing.T) {
	harness := newM5Harness(t)
	for _, name := range []string{
		"node_runs_ready_idx",
		"node_runs_ready_fifo_idx",
		"node_attempts_project_inflight_idx",
		"node_attempts_scheduling_inflight_idx",
	} {
		var exists bool
		if err := harness.client.Pool().QueryRow(harness.ctx, `
			SELECT EXISTS (
			  SELECT 1 FROM pg_indexes
			  WHERE schemaname=current_schema() AND indexname=$1
			)`, name).Scan(&exists); err != nil {
			t.Fatal(err)
		}
		if exists {
			t.Fatalf("scheduler-only index %s still exists", name)
		}
	}
}
