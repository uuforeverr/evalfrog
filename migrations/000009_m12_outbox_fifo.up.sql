-- available_at controls eligibility and retry backoff; it must not replace the
-- immutable business arrival order once a row is eligible for publication.
DROP INDEX outbox_events_relay_idx ON outbox_events;
CREATE INDEX outbox_events_relay_idx
	ON outbox_events(published_at, occurred_at ASC, event_id ASC,
	                 available_at, claim_token, claim_expires_at);

DROP INDEX node_task_outbox_relay_idx ON node_task_outbox;
CREATE INDEX node_task_outbox_relay_idx
	ON node_task_outbox(published_at, occurred_at ASC, task_id ASC,
	                    available_at, claim_token, claim_expires_at, resource_class);
