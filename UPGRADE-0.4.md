# Upgrade from 0.3.x to 0.4.x

Version 0.4 is an incremental release on top of the 0.3 architecture. Nothing changes in
the message model, bus names, DSN schemes, RabbitMQ topology, Redis key shapes or the
supervisor command output. In-flight messages remain readable — no queue drain is
required. The two changes below are a schema addition and a behavioral improvement built
on it.

---

## 1. Schema: new `available_at` column on the workflow message tables

Every workflow outbox/inbox table (default `zz_*`) gains a nullable `available_at`
timestamp column with an index. Apply it in one of these ways:

- `bin/console messenger:setup-transports` — the transports' schema update applies the
  diff (adds the column and index to existing tables, no data is touched); or
- if your application manages schema through Doctrine migrations and the messenger
  tables are part of the mapped schema, `doctrine:migrations:diff` picks the change up;
  or
- manually, per table (PostgreSQL shown; repeat for every workflow inbox/outbox table):

  ```sql
  ALTER TABLE zz_commands_inbox ADD available_at TIMESTAMP(0) WITHOUT TIME ZONE DEFAULT NULL;
  CREATE INDEX IDX_zz_commands_inbox_available_at ON zz_commands_inbox (available_at);
  ```

Deploy order is uncritical: 0.3 code ignores the extra column, and 0.4 treats a `NULL`
`available_at` as "immediately available" — but the column must exist before 0.4 workers
start (the first retry redelivery writes it).

## 2. Retry backoff now applies on the inbox hop

In 0.3 the configured retry delays only shaped **broker** hops; on the inbox hop retries
were immediate, so a deterministically failing handler burned its whole attempt budget in
seconds (documented in `UPGRADE-0.3.md` §6). In 0.4 the retry redelivery persists the
retry strategy's delay as the row's `available_at`:

- **Competing-consumer transports** (commands default): rows in backoff are skipped by
  the availability filter; other messages flow past them.
- **Single-consumer FIFO transports** (events default, `strict_order`): a message in
  backoff **blocks its successors** — ordering is preserved, the queue waits. The wait is
  bounded by the flow's total retry budget.

Consequences to review:

- **Event queues drain to the DLQ more slowly.** A poison event previously exhausted its
  ~20 attempts near-instantly and unblocked the queue in seconds; it now genuinely waits
  out the backoff schedule (default budget: 15 minutes) while blocking its ordered queue.
  If that latency matters, tune `messenger_workflow.messenger.defaults.event_retry`
  (`max_total_delay`, `max_retries`) and list deterministic domain/validation exceptions
  in `non_retryable_exceptions` so real bugs go to the DLQ on the first attempt.
- **PostgreSQL pickup latency:** a worker sleeping on LISTEN/NOTIFY notices a due retry
  at its next `check_delayed_interval` re-poll (default 60 s) — a short backoff delay can
  therefore stretch up to that interval. Lower `check_delayed_interval` on transports
  where precise retry timing matters.
- `getMessageCount()` / `findAll()` on competing-consumer transports no longer count/list
  rows in backoff (they count *available* messages, consistent with what `get()` would
  deliver).
- Initial sends are unchanged: dispatching with a `DelayStamp` onto an outbox/inbox
  transport still throws (`Delay is not supported!`) — `available_at` is written only by
  retry redeliveries.

For anyone extending the internals: `Connection::update()` gained an optional
`?\DateTimeImmutable $availableAt` fourth parameter — subclasses overriding it must adopt
the new signature.

## 3. Recorded decision: DLQ replays keep bypassing inbox dedup

`messenger:failed:retry` continues to re-execute handlers without consulting the dedup
index — deliberately: every DLQ'd message's UUID is already marked processed (a permanent
rejection stamps the index like an ack), so a dedup check would block all replays, and no
marker can distinguish "failed before applying side effects" from "failed after". See
"Failure transports (DLQ) and replays" in the README for the operational contract
(idempotent handlers, `transactional_handler=true`).
