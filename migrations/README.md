# Migrations

The migration runner targets a profile-isolated MySQL 8.4 database and records immutable checksums in `schema_migrations`. The numbered files build Access, Definition, Runtime, Outbox/Inbox, Task dispatch, Managed Resources, product surface, recovery, audit and final relay indexes in dependency order.

M12 originally appended `000007_scheduler_fifo_batching.up.sql` for Scheduler access paths. The immutable migration remains in history; `000008_engine_direct_task_dispatch.up.sql` removes those indexes after Engine takes ownership of atomic `Ready -> Queued + Attempt + Task Outbox` persistence and Scheduling Redis is retired.
`000009_m12_outbox_fifo.up.sql` then rebuilds the Runtime/Task Outbox relay indexes around immutable `occurred_at + stable ID` order; `available_at` remains only an eligibility and retry-backoff filter.

This engine cutover supports fresh MySQL databases only; it does not migrate PostgreSQL production data. MySQL DDL is atomic per statement, not across an entire migration file. A failed fresh initialization must be retried against a new empty database after the SQL is corrected. Once the MySQL baseline is released, migration files are immutable and append-only; editing an applied file causes a checksum mismatch.
