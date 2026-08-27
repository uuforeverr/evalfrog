-- M12 replaces per-Project Lane/Credit planning with one global oldest-Ready
-- reconciliation read. Both indexes are partial because terminal history must
-- not enlarge the Scheduler's hot access paths.
CREATE INDEX node_runs_ready_fifo_idx
    ON node_runs(ready_at, project_id, node_run_id)
    INCLUDE (run_id, execution_node_id, state_version, priority,
             operation_type, operation_version, resource_class)
    WHERE state = 'ready';

CREATE INDEX node_attempts_scheduling_inflight_idx
    ON node_attempts(project_id, attempt_id)
    INCLUDE (run_id, node_run_id, state)
    WHERE state IN ('queued', 'running');
