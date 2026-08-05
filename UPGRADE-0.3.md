# Upgrade from 0.2.x to 0.3.x

Version 0.3 is a ground-up rewrite of the bundle's internals. What is **kept**:

- Package name (`kraz/messenger-workflow`), namespace (`Kraz\MessengerWorkflow\`), bundle
  class and config root (`messenger_workflow`).
- The message model (commands, queries, domain events), the handler attributes
  (`#[AsCommandHandler]`, `#[AsQueryHandler]`, `#[AsEventHandler]`) and the bus names
  (`command.bus`, `query.bus`, `event.bus`).
- The custom transport DSN schemes (`commands-outbox://`, `commands-inbox://`,
  `commands-failures://`, `events-outbox://`, `events-inbox://`, `events-failures://`,
  DSN host = Doctrine DBAL connection name) and the default `zz_*` table names.
- The RabbitMQ topology and routing-key derivation (direct exchanges for commands/queries,
  topic exchange for events, `Contracts\*` public vs `internal.` prefixed keys) — existing
  brokers and foreign consumers keep working.
- The Redis key shapes (`rs:[<namespace>:]<taskId>`, `task-meta:<taskId>`) — stored results
  and task metadata survive the upgrade.
- The `messenger:supervisor-config` command name and output format.
- The `QueryBusInterface` API — `ask()`, `askAsync()`, `await()` are unchanged.

What **changed** is listed below, roughly in the order you will meet it while upgrading.

---

## 0. Before you deploy: drain your queues

There is **no data migration** for in-flight messages. The message envelopes serialized
into the outbox/inbox tables and the broker queues carry 0.2 stamp headers that 0.3
cannot deserialize.

1. Stop producing messages (maintenance mode; stop web/cron traffic).
2. Keep the 0.2 workers running until all outboxes, broker queues and inboxes are empty
   (watch `messenger:stats` and the RabbitMQ management UI).
3. Empty the failure transports: retry (`messenger:failed:retry`) or discard
   (`messenger:failed:remove`) every failed message — failed rows are not readable after
   the upgrade.
4. Stop all workers.
5. Deploy 0.3, then **drop the old `zz_*` tables** (outboxes, inboxes, inbox `*_index`
   tables, failures) and run `bin/console messenger:setup-transports` to create the new
   schema.
6. Regenerate the supervisord configuration (`bin/console messenger:supervisor-config`)
   and start the workers again.

Redis needs no action: result-storage and task-meta keys are byte-compatible.

## 1. `CommandBusInterface::dispatch()` is now always asynchronous

The 0.2 command bus executed a locally registered handler synchronously in-process and
only fell back to the broker for remote handlers. **Synchronous local execution is gone**:
every command, query and event goes through the message broker, always. `dispatch()`
never blocks and never throws task exceptions; `dispatchAsync()` has been removed.

```php
// 0.2.x
$commandBus->dispatch(new RegisterBook('978-3-16'), timeout: 30); // blocked, threw on failure
$taskId = $commandBus->dispatchAsync(new RegisterBook('978-3-16'));
$commandBus->await($taskId, 30);

// 0.3.x
$commandBus->dispatch(new RegisterBook('978-3-16'));          // fire-and-forget, untracked
$commandBus->dispatch(new RegisterBook('978-3-16'), $taskId); // tracked: $taskId is set (UUID v7)
$commandBus->await($taskId, 30);                              // the only way to wait; void,
                                                              // throws TaskFailedException /
                                                              // TaskTimeOutException
```

- Result tracking is opt-in and activated by **passing the second argument** (by
  reference), not by its value. Untracked dispatches produce zero result-storage,
  notifier and Redis artifacts anywhere in the flow.
- If you decorate the command bus, your decorator must forward the *presence* of the
  second argument (`func_num_args()`), not just its value.
- The default `await()` timeout is still 300 s and is now configurable:
  `messenger_workflow.messenger.defaults.await_timeout`.
- `await()` now refuses to deadlock: awaiting a task whose message still sits in an
  outbox inside your own uncommitted database transaction throws
  `PendingOutboxMessageException` instead of timing out 300 s later.

Because nothing runs in-process anymore, **workers must be running even in development**
for commands/queries/events to be handled.

## 2. Reading command task results

`CommandBus::await()` still returns `void`. In 0.2 the result *value* of a command task
was unreachable without custom app-side services reading Redis directly. 0.3 ships task
services as part of the package — enable them and delete your app-side duplicates:

```yaml
messenger_workflow:
    messenger:
        result_storage:
            provider: redis
            service: snc_redis.mwf_cache   # your \Redis client service id
        tasks:
            enabled: true                  # NEW — requires the redis provider
```

This registers (interfaces autowireable, key shapes unchanged from 0.2):

- `Application\Task\TaskStatusProviderInterface` — non-destructive status peek
  (`pending` / `completed` / `failed`, or `TaskNotFoundException`),
- `Application\Task\TaskResultProviderInterface` — non-blocking read of a completed
  task's value (this is how you read command results),
- `Application\Task\TaskOwnershipRegistryInterface` — `task-meta:<id>` ownership records,
- tracking decorators for the command/query buses that record ownership automatically;
  provide an optional `Application\Task\TaskOwnerResolverInterface` service to attach an
  owner (no dependency on symfony/security).

## 3. Handler transactions: `HandlerTransactionInterface` is gone

0.2 injected transaction objects into handlers via handler arguments. 0.3 manages the
transaction in middleware — handlers are plain callables:

```php
// 0.2.x
public function __invoke(RegisterBook $command, ?TransportHandlerTransaction $transaction = null) { ... }

// 0.3.x
public function __invoke(RegisterBook $command) { ... }
```

- Remove `HandlerTransactionInterface` / `TransportHandlerTransaction` parameters and
  service references; the classes no longer exist.
- When a message is received from a transactional inbox, the middleware opens a
  transaction on the inbox's DBAL connection, runs the handler inside it, deletes the
  inbox row in the same transaction and commits. Application writes on the same
  connection (= the same bounded-context database) join automatically.
- Handlers can observe the transaction through their injected connection
  (`$connection->isTransactionActive()`) if they need to.
- Commands: transactional by default. Events: opt in per transport with the
  `transactional_handler=true` DSN option.
- Without an inbox, map the broker queue to an entity manager (or DBAL connection) name
  and the middleware wraps handlers in a plain transaction:

  ```yaml
  messenger_workflow:
      messenger:
          transports:
              commands:
                  orm_mappings:
                      book_store_commands: { orm: book_store }
  ```

## 4. Moved and removed classes

The public API (`Domain\*`, `Application\*` interfaces, attributes, exceptions) kept its
FQCNs. Everything infrastructure moved from `Kraz\MessengerWorkflow\Messenger\...` into
`Kraz\MessengerWorkflow\Infrastructure\...`. Classes apps commonly referenced:

| 0.2.x | 0.3.x |
|---|---|
| `Messenger\Transport\ResultStorageInterface` | `Application\Task\ResultStorageInterface` |
| `Messenger\Transport\ResultStoragePayload` | `Application\Task\ResultStoragePayload` |
| `Messenger\Transport\Exception\ResultStorageWaitTimeoutException` | `Application\Task\Exception\ResultStorageWaitTimeoutException` |
| `Messenger\Transport\InMemoryResultStorage` | `Infrastructure\Task\InMemoryResultStorage` |
| `Messenger\Transport\RedisResultStorage` | `Infrastructure\Task\RedisResultStorage` |
| `Messenger\CommandBus` / `QueryBus` / `EventBus` | `Infrastructure\Messenger\CommandBus` / `QueryBus` / `EventBus` |
| `Messenger\Event\CommandCompletedNotification` | `Infrastructure\Messenger\CommandCompletedNotification` |
| `Messenger\RoutingKey` | `Infrastructure\Messenger\RoutingKey` |
| `Messenger\Stamp\MessageIdStamp` | `Infrastructure\Messenger\Stamp\MessageIdStamp` |
| `Messenger\Stamp\StrictOrderStamp` | `Infrastructure\Messenger\Stamp\StrictOrderStamp` |
| `Messenger\Console\MessengerSupervisorConfigCommand` | `Infrastructure\Console\MessengerSupervisorConfigCommand` |

Removed without replacement (superseded by the new design):

- `Application\Messenger\HandlerTransactionInterface`,
  `Messenger\Transport\Handler\TransportHandlerTransaction`,
  `Messenger\Doctrine\Handler\DoctrineHandlerTransaction` (see §3);
- the copied AMQP transport (`Messenger\Amqp\*`) — the stock
  `jwage/phpamqplib-messenger` transport is used unmodified;
- the flow middlewares `CommandMiddleware`, `QueryMiddleware`, `EventMiddleware`,
  `InboxMiddleware`, `OutboxMiddleware` and the stamps `AsyncMessageStamp`,
  `AwaitMessageResultStamp`, `CommandStamp`, `EventStamp`, `QueryStamp`,
  `ResultStorageStamp`, `SourceTransportNameStamp`, `TargetTransportNameStamp` —
  replaced by native Symfony routing plus a much smaller internal stamp/middleware set;
- `Messenger\Doctrine\DbalConnectionComparator`,
  `Messenger\Transport\TransactionalTransportInterface`.

Internal service id rename: `messenger_workflow.add_message_id_stamp_middleware` →
`messenger_workflow.message_id_middleware`.

## 5. Workers

The `workflow.workers` section is now **optional**: the worker set is derived from your
transport topology (outbox → publisher worker, broker queue + inbox → receiver + handler
pair, no inbox → one collapsed consumer per queue, `*_notifier` outbox → notifier worker,
queries queue → query handler worker). Your existing hand-written declarations keep
working — manual entries match derived workers by name or by `type|source|queue` identity
and override them; `enabled: false` removes a derived worker; a free-form `labels` map
can be attached.

Two **default target buses changed** — only configs that explicitly pinned the old
values need editing:

| Worker type | 0.2.x default target | 0.3.x default target |
|---|---|---|
| `event_publisher` | `event.bus` | `relay.bus` |
| `command_notifier` | `outbox.bus` | `notifier.bus` |

The old internal buses `outbox.bus`/`inbox.bus` are replaced by the internal
`relay.bus` (outbox → broker), `inbox.bus` (broker → inbox) and `notifier.bus`
(notifier outbox → result storage). If you run `messenger:consume` by hand, note that
outbox rows carry no bus-name stamp — relay/notifier/receiver workers need an explicit
`--bus=...` option (the generated supervisord config does this for you).

`messenger:supervisor-config` output is unchanged (a doubled
`environment=environment=...` line produced by custom `supervisor.environment` overrides
was fixed); a new `--output-dir` option writes one `<group>.conf` file per worker group
instead of printing to stdout.

## 6. Retry policies

0.2 retried according to whatever `retry_strategy` each transport configured. 0.3 wires
opinionated per-flow policies automatically (an explicit `retry_strategy` on a transport
still wins):

- **Commands**: no retries — except transient infrastructure errors (connection loss,
  deadlocks, broker/transport exceptions): ≤ 3 attempts, exponential backoff + jitter,
  ~30 s total budget. Permanent failure → failure transport, and the error is written to
  the result storage for tracked commands.
- **Events**: always retried with backoff + jitter inside a bounded total budget
  (default 15 min), then the failure transport — a poison message cannot block an
  ordered queue forever.
- **Queries**: aggressive fast retries on transient errors only; **no failure
  transport** — a permanent failure is reported to the asker (`TaskFailedException`)
  and the message is dropped.

Tune per flow under `messenger_workflow.messenger.defaults.{command,query,event}_retry`
(`retryable_exceptions` / `non_retryable_exceptions` class lists included), or register
a service implementing `Infrastructure\Messenger\Retry\RetryDeciderInterface` to join
the decision chain. `RecoverableMessageHandlingException` /
`UnrecoverableMessageHandlingException` keep their native semantics.

Note on delays: on the **inbox** hop retries are immediate — inbox rows carry no delay
column (same limitation as 0.2), so the retry budget there is enforced by attempt count
and a wall-clock cap. The configured backoff delays take full effect on the broker hops
(AMQP delay exchanges): queries and the no-inbox flows.

## 7. Result storage behavior

- 0.2 silently re-expired a result **60 seconds after a successful `await()`**, which
  could delete a result other consumers still needed. 0.3 never shortens the result TTL
  on await. If you depended on the old behavior, opt in with
  `result_storage.expire_after_await: <seconds>`.
- New keys: `result_storage.namespace` (was constructor-only) and
  `result_storage.expire_input_after` (result TTL, default 10800 s).

## 8. New in 0.3 (no action required)

- `messenger_workflow.messenger.outbox_buses` registers per-context
  `OutboxBusInterface` services (`<context>: <outbox transport>`); the old hand-written
  outbox-bus pattern still works.
- `strict_order=true` DSN option for ordered transports; combining it with
  `multiple_consumers=true` fails at container compile time.
- The inbox/outbox transports support the worker keepalive mechanism
  (`messenger:consume --keepalive`): a long-running handler periodically refreshes its
  in-flight marker instead of being redelivered to a competing consumer after
  `redeliver_timeout`.
- Any flow segment (outbox, inbox, notifier) can be removed per configuration — see the
  README's flow-reduction section for the dual-write and idempotency trade-offs.
- Exactly-one-handler enforcement for commands/queries at consume time; zero handlers on
  a consumed tracked command/query reports the error to the asker.
