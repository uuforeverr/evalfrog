ALTER TABLE workflow_runs
    ADD COLUMN cancel_requested_at DATETIME(6);

CREATE INDEX workflow_runs_project_updated_idx
    ON workflow_runs(project_id, updated_at DESC, run_id);
