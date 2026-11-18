-- M12 replaces per-Project Lane/Credit planning with one global oldest-Ready
-- reconciliation read. MySQL has no partial indexes, so state leads each
-- composite access path.
CREATE INDEX node_runs_ready_fifo_idx
	ON node_runs(state, ready_at, project_id, node_run_id,
	             run_id, execution_node_id, state_version, priority);

CREATE INDEX node_attempts_scheduling_inflight_idx
	ON node_attempts(state, project_id, attempt_id, run_id, node_run_id);
