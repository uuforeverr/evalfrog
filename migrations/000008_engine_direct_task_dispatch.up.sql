-- Engine now persists a newly runnable Task directly as Queued together with
-- its Attempt and Task Outbox record. Ready nodes and scheduling inflight scans
-- are no longer runtime access paths.
DROP INDEX node_runs_ready_idx ON node_runs;
DROP INDEX node_runs_ready_fifo_idx ON node_runs;
DROP INDEX node_attempts_project_inflight_idx ON node_attempts;
DROP INDEX node_attempts_scheduling_inflight_idx ON node_attempts;
