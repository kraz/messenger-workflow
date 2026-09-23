# MessengerWorkflow

> [!WARNING]
> This Symfony bundle is a proof of concept. Using it in production is not recommended!

A Symfony bundle implementing Enterprise Integration Patterns on top of Symfony Messenger:
CQRS message buses, transactional outbox/inbox, tracked async tasks and RabbitMQ integration.
It is the messaging backbone for a modular monolith whose bounded contexts (modules) communicate
only asynchronously — so they can later be split into independently deployed applications without
touching application code.

Upgrading? Read **[UPGRADE-0.5.md](UPGRADE-0.5.md)** (and [UPGRADE-0.4.md](UPGRADE-0.4.md),
[UPGRADE-0.3.md](UPGRADE-0.3.md) for older releases).

## Message model

- **Commands** — change state, fire-and-forget, exactly one handler. Optionally *tracked*:
  dispatching with a by-reference task-id argument makes the result awaitable/pollable by UUID.
- **Queries** — return a result, no side effects, exactly one handler. Always tracked (the
  result storage is their return channel).
- **Domain events** — immutable notifications about domain changes. Zero or more handlers,
  fanned out per bounded context via topic bindings.

Every message goes through the message broker — there is no synchronous in-process handling,
so behavior is identical whether the handler lives in the same deployment or in another one.

## Flows

```
Command: dispatch() → [outbox ⇄ publisher worker] → RabbitMQ (direct exchange "commands")
         ⇄ [receiver worker → inbox ⇄] handler worker (TX: inbox delete + handler writes)
         → [notifier outbox ⇄ notifier worker] → result storage   (tracked commands only)

Event:   publish() → [outbox ⇄ publisher worker] → RabbitMQ (topic exchange "events")
         ⇄ [receiver worker → inbox ⇄] handler worker (TX optional)

Query:   ask()/askAsync() → RabbitMQ (direct exchange "queries") → handler worker → result storage
```

Segments in `[...]` are optional per configuration — see [Reducing the flow](#reducing-the-flow).
`⇄` marks the at-least-once hand-offs: a failed hand-off keeps the message on the safe side.

Every flow variant is traced end to end — configuration, derived workers, the exact worker
commands and what happens at each hop — in **[MESSAGE_FLOWS.md](MESSAGE_FLOWS.md)**.

## Features

- **Transactional outbox** (`*-outbox://` transports): messages are stored with your domain
  changes in one database transaction and relayed to RabbitMQ by a publisher worker — at-least-once
  delivery even when the broker is down. FIFO by auto-increment id; PostgreSQL `LISTEN/NOTIFY`
  wakes idle relays instantly. Awaiting a task whose message still sits in your own uncommitted
  transaction throws `PendingOutboxMessageException` instead of deadlocking.
- **Inbox with deduplication** (`*-inbox://` transports): a receiver worker moves broker messages
  into the handling context's database; a message UUID is handled *at most once* — broker
  redeliveries of already-processed UUIDs are dropped, even when the processing had failed
  permanently. Handlers run **inside the transaction that deletes the inbox row**, so application
  writes on the same connection commit atomically with the message consumption. Competing
  consumers via `FOR UPDATE SKIP LOCKED` (commands default) or single-consumer FIFO
  (events default); long-running handlers stay protected from redelivery via
  `messenger:consume --keepalive`.
- **Tracked tasks**: tracked command results and all query results land in the result storage
  (Redis in production, in-memory for tests/dev) under `rs:[<ns>:]<uuid>`. Optional task services
  add ownership records, a status provider (`pending|completed|failed`, plus `unknown` for a
  result that exists but cannot be decoded — never misreported as success) and a non-blocking result
  provider.
- **Opinionated retry policies** per flow (commands / queries / events) with transient-error
  detection, exception-class lists, custom decider services, exponential backoff + jitter and
  bounded total budgets; failure transports (DLQ) for commands and events.
- **Worker auto-derivation**: the required workers (publisher, receiver, handler, notifier, query
  handler) are derived from the transport topology; `messenger:supervisor-config` renders the
  supervisord configuration (stdout or `--output-dir`). Manual worker declarations override the
  derived set.
- **RabbitMQ integration** on the stock `jwage/phpamqplib-messenger` transport (no subclassing):
  direct exchanges for commands/queries, topic exchange for events; routing keys derived from the
  message FQCN (`Contracts\*` = public contract, everything else `internal.<Context>`); queue
  binding keys derived from the queue owner and the `#[AsEventHandler(fromTransport: ...)]`
  handler declarations.
- **Dedicated queues (routes)**: send chosen command or query classes of a context to a queue of
  their own — e.g. a single-consumer FIFO lane for ordering-sensitive commands next to the
  competing regular queue — by listing them on the queue binding (`route` + `messages`) or marking
  them with `#[MessageRoute]`. Exclusive by construction (the routing key carries the route), with
  its own inbox table, workers and DLQ, sharing the context's notifier.
- **Multi-database support**: one database per bounded context; the DSN host of every
  outbox/inbox transport is the Doctrine DBAL connection name, so the owning database is always
  inferable from the transport.

## Installation

```bash
composer require kraz/messenger-workflow
```

Requires PHP ≥ 8.4, Symfony ^8.1, `ext-redis`, PostgreSQL, RabbitMQ and Redis. Register the
bundles:

```php
// config/bundles.php
Kraz\MessengerWorkflow\MessengerWorkflowBundle::class => ['all' => true],
```

## Configuration

The bundle prepends the three buses (`command.bus`, `query.bus`, `event.bus`), the broker
transports (`commands`, `queries`, `events` — you supply the DSNs), the marker-interface routing
and the serializer defaults. A typical bounded context ("book_store", own database) adds:

```yaml
framework:
    messenger:
        transports:
            commands: { dsn: '%env(MSG_BROKER_DSN)%' }   # AMQP; exchange/serializer options prepended
            queries:  { dsn: '%env(MSG_BROKER_DSN)%' }
            events:   { dsn: '%env(MSG_BROKER_DSN)%' }

            # host = Doctrine DBAL connection name
            book_store_commands:                          # inbox, named after the broker queue
                dsn: 'commands-inbox://book_store'
                failure_transport: book_store_commands_failures
            book_store_commands_failures: 'commands-failures://book_store?queue_name=book_store_commands'
            book_store_commands_notifier: 'commands-outbox://book_store?table_name=zz_commands_notifier'
            book_store_outbox: 'events-outbox://book_store'
            book_store_events:
                dsn: 'events-inbox://book_store'
                failure_transport: book_store_events_failures
            book_store_events_failures: 'events-failures://book_store?queue_name=book_store_events'

messenger_workflow:
    messenger:
        transports:
            commands:
                queue_bindings:
                    - { queue: book_store_commands, owner: BookStore }
            queries:
                queue_bindings:
                    - { queue: book_store_queries, owner: BookStore }
            events:
                queue_bindings:
                    # binding keys for foreign events are auto-derived from
                    # #[AsEventHandler(fromTransport: 'book_store_events')] handlers,
                    # or listed explicitly via binding_keys: [...]
                    - { queue: book_store_events, owner: BookStore }
        outbox_buses:
            book_store: book_store_outbox    # registers an OutboxBusInterface for the context
        result_storage:
            provider: redis
            service: snc_redis.mwf_cache     # \Redis client service id
        tasks:
            enabled: true                    # ownership/status/result providers + tracking buses
```

Provision the broker topology and the database tables once per deploy:

```bash
bin/console messenger:setup-transports
```

The full configuration shape (all keys, defaults and descriptions) is generated into
[`config/reference.php`](config/reference.php).

### Full default configuration

All keys of the `messenger_workflow` extension with their default values and inlined
comments (cross-check anytime with `bin/console config:dump-reference messenger_workflow`):

```yaml
messenger_workflow:
    messenger:

        # Broker transport add-ons, keyed by the framework.messenger transport name.
        # Only meaningful for the AMQP broker transports ("commands", "queries", "events").
        transports:

            # Prototype — example key: "commands"
            name:

                # Maps broker queues to entity manager names (used when handling WITHOUT
                # an inbox): the workflow transaction middleware wraps the queue's
                # handlers in a plain transaction on the mapped entity manager's (or,
                # as a fallback, DBAL connection's) database.
                orm_mappings: []
                    # book_store_commands: book_store          # short form
                    # book_store_commands: { orm: book_store } # long form

                # Queues of this broker transport: each entry declares the owning
                # bounded context and optional explicit binding keys. Binding keys are
                # derived from the owner (own internal messages; "<ctx>.#" wildcard on
                # the topic exchange, public + internal key on direct exchanges), the
                # #[AsEventHandler(fromTransport: ...)] handler declarations (topic
                # exchange only) and the explicit "binding_keys" list.
                # Direct exchanges only: "route" makes the queue a dedicated queue that
                # receives ONLY the classes listed under "messages" (or marked with
                # #[MessageRoute('<route>')]); it binds to the owner keys suffixed with
                # the route. "notifier" (commands) names the outbox its tracked results
                # go through — default: the notifier of the owner's regular queue;
                # false: none, on purpose.
                queue_bindings: []
                    # - { queue: book_store_commands, owner: BookStore }
                    # - { queue: book_store_bulk, owner: BookStore, route: bulk,
                    #     messages: ['App\BookStore\Application\Command\RebuildCatalog'] }
                    # - { queue: book_store_events, owner: BookStore,
                    #     binding_keys: ['Contracts\Billing\Event\InvoicePaid'] }

        defaults:

            # Default await() timeout in seconds for command/query tasks.
            await_timeout: 300

            # Command retry policy: no retries by default; transient infrastructure
            # errors are retried with exponential backoff and jitter within a small
            # total time budget. Applied to commands inboxes — or, in no-inbox mode,
            # to the "commands" broker transport itself.
            command_retry:
                transient_max_retries: 3
                delay: 1000               # initial retry delay in milliseconds
                multiplier: 2.0
                max_delay: 10000          # max delay per retry in ms (0 = uncapped)
                jitter: 0.1               # randomness applied to the delay (0..1)
                max_total_delay: 30000    # total retry-time budget in ms (0 = unbounded)
                # Exception classes (instanceof match) that force a retry.
                retryable_exceptions: []
                # Exception classes (instanceof match) that force a permanent failure.
                # Wins over retryable_exceptions.
                non_retryable_exceptions: []

            # Query retry policy: aggressive fast retries on transient infrastructure
            # errors only. No failure transport — a permanent failure is reported to
            # the asker through the result storage and the message is dropped.
            query_retry:
                transient_max_retries: 10
                delay: 100
                multiplier: 2.0
                max_delay: 5000
                jitter: 0.1
                max_total_delay: 60000
                retryable_exceptions: []
                non_retryable_exceptions: []

            # Event retry policy: always retried (exponential backoff and jitter)
            # within a bounded total time budget, then DLQ — a poison message cannot
            # block an ordered queue forever.
            event_retry:
                max_retries: 20
                delay: 1000
                multiplier: 2.0
                max_delay: 60000
                jitter: 0.1
                max_total_delay: 900000   # 15 minutes
                retryable_exceptions: []
                non_retryable_exceptions: []

        # Per-context outbox buses: "<context>: <outbox transport name>" registers an
        # OutboxBusInterface implementation "messenger_workflow.outbox_bus.<context>",
        # autowirable as "OutboxBusInterface $<context>OutboxBus" (single entries also
        # alias the bare interface).
        outbox_buses: []
            # book_store: book_store_outbox

        result_storage:
            # "memory" (single-process, tests/dev), "redis", or a custom suffix
            # resolved as service "messenger_workflow.result_storage.<provider>".
            provider: memory
            # Service id of the \Redis client used by the redis provider.
            service: null             # default "redis_client.default"
            # Optional key namespace: results are stored as rs:<namespace>:<taskId>.
            namespace: null
            # TTL in seconds applied to stored results.
            expire_input_after: 10800 # 3 hours
            # If set, a successful await() re-expires the result after this many
            # seconds. Off by default: awaiting never shortens the result TTL.
            expire_after_await: null

        workflow:

            # Defaults merged into every worker (derived and manual) — same keys as a
            # "workers" entry; typically used for shared supervisor options.
            worker_defaults: []
                # supervisor: { autostart: true, autorestart: true }

            # Manual worker declarations. They MERGE with the auto-derived set: an
            # entry matching a derived worker by name or by type+source+queue identity
            # overrides it; unmatched entries are added; "enabled: false" removes.
            workers: []
                # -
                #     name: Book store commands handler
                #     group: book_store       # supervisord group (default: derived context)
                #     # One of: event_publisher, event_receiver, event_handler,
                #     # command_receiver, command_handler, command_notifier, query_handler.
                #     # The type presets source/target buses; *_publisher/*_handler/
                #     # *_notifier require "source", *_receiver/query_handler require "queue".
                #     type: command_handler
                #     source: book_store_commands   # transport consumed (messenger:consume <source>)
                #     target: command.bus           # bus dispatched on (--bus=<target>; preset by type)
                #     queue: ''                     # broker queue (--queues=<queue>)
                #     enabled: true                 # false removes a derived worker
                #     instances: 1                  # >1 requires a competing-consumer source
                #     labels: []                    # free-form metadata (e.g. for deploy tooling)
                #     cmd_extra_options:            # rendered as messenger:consume options
                #         limit: ~                  # --limit
                #         failure_limit: ~          # --failure-limit
                #         memory_limit: ~           # --memory-limit
                #         time_limit: ~             # --time-limit
                #         fetch_size: ~             # --fetch-size (min 1)
                #         sleep: ~                  # --sleep (receivers/query handlers default to 0)
                #         # --keepalive: a long-running handler refreshes its in-flight
                #         # marker instead of being redelivered after redeliver_timeout.
                #         # Only meaningful on multiple_consumers=true sources; keep it
                #         # below the transport's redeliver_timeout.
                #         keepalive: ~
                #         verbose: ~                # e.g. "-vv"
                #     supervisor: []                # raw supervisord program options

        tasks:
            # Registers the Redis task services (ownership registry, status/result
            # providers) and the tracking bus decorators. Requires the "redis"
            # result_storage provider (shares its client and key namespace).
            enabled: false
            # TTL in seconds of task ownership records — keep it above the result TTL
            # so ownership outlives results.
            ownership_ttl: 14400      # 4 hours
            # Expose exception class names in task errors.
            debug: null               # default: %kernel.debug%
```

### Transport DSN options

| Option                                         | Transports                 | Default                         | Meaning                                                                             |
|------------------------------------------------|----------------------------|---------------------------------|-------------------------------------------------------------------------------------|
| `table_name`                                   | outbox, inbox, failures    | `zz_commands_*` / `zz_events_*` | storage table (inbox dedup index table = `<table_name>_index`). Defaults are per **scheme**: two inbox or outbox transports on one connection must not share a table — the container build fails if they do; the inbox of a routed queue defaults to `<scheme default>_<transport name>` |
| `multiple_consumers`                           | inbox                      | commands `true`, events `false` | competing consumers via `FOR UPDATE SKIP LOCKED`                                    |
| `strict_order`                                 | inbox, outbox              | `false`                         | request single-consumer FIFO; conflicts with `multiple_consumers` (boot-time error) |
| `transactional_handler`                        | inbox                      | commands `true`, events `false` | run handlers inside the inbox-row-deleting transaction                              |
| `redeliver_timeout`                            | inbox, outbox              | `300`                           | seconds before an unacked in-flight row is redelivered                              |
| `get_notify_timeout`, `check_delayed_interval` | inbox, outbox (PostgreSQL) | `60000`                         | LISTEN/NOTIFY wait and re-poll interval (ms)                                        |
| `queue_name`                                   | failures                   | —                               | scopes the stock Doctrine failure transport per queue                               |

## Usage

```php
final class BookController
{
    public function __construct(
        private CommandBusInterface $commandBus,
        private QueryBusInterface $queryBus,
    ) {}

    public function register(): Response
    {
        // fire-and-forget
        $this->commandBus->dispatch(new RegisterBook('978-3-16'));

        // tracked: $taskId (UUID v7) is assigned; poll it or await it
        $this->commandBus->dispatch(new RegisterBook('978-3-16'), $taskId);
        $this->commandBus->await($taskId, timeout: 30);   // void; throws TaskFailedException / TaskTimeOutException

        // queries
        $result = $this->queryBus->ask(new GetBook('978-3-16'));       // blocking round-trip
        $taskId = $this->queryBus->askAsync(new GetBook('978-3-16'));  // ...or split
        $result = $this->queryBus->await($taskId, timeout: 10);
    }
}
```

Messages are plain classes implementing the marker interfaces `Application\CommandInterface`,
`Application\QueryInterface` or `Domain\DomainEventInterface`. Handlers:

```php
#[AsCommandHandler]
final class RegisterBookHandler
{
    public function __invoke(RegisterBook $command): string   // return value = task result (tracked commands)
    {
        // runs inside the inbox transaction — writes on the context's connection commit atomically
        return $bookId;
    }
}

#[AsEventHandler(fromTransport: 'book_store_events')]         // scope to this context's queue
final class BookRegisteredHandler
{
    public function __invoke(BookRegistered $event): void { /* ... */ }
}
```

The attributes also work on methods, and a handler method may declare extra parameters
after the message — they are resolved from the container with full autowiring semantics
(`#[Autowire]`/`#[Target]` included; nullable parameters degrade to null when no service
matches, defaults are kept). This lets a controller class host its feature's handler
without constructor injection:

```php
#[AsController]
final class RegisterBookController
{
    #[Route(path: '/register', methods: ['POST'])]
    public function registerBook(#[MapRequestPayload] RegisterBookRequest $request, CommandBusInterface $commandBus): void
    {
        $commandBus->dispatch(RegisterBookCommand::fromRequest($request));
    }

    #[AsCommandHandler]
    public function registerBookHandler(RegisterBookCommand $command, BookRepositoryInterface $books): void
    {
        $books->add(Book::register($command));
    }
}
```

(Batch handlers keep Symfony's native `($message, Acknowledger)` signature and are
never wrapped.)

Domain events are published through the context's outbox bus (atomic with the domain
transaction) or directly to the event bus (immediate, less resilient):

```php
$this->outboxBus->publish($event);   // OutboxBusInterface, from messenger_workflow.messenger.outbox_buses
$this->eventBus->publish($event);    // EventBusInterface — straight to the broker
```

Task status and results (e.g. for a polling HTTP endpoint), with `tasks.enabled: true`:

```php
$status = $this->taskStatusProvider->getStatus($taskId);   // TaskStatusResponse: pending|completed|failed
$value = $this->taskResultProvider->getResult($taskId);    // completed command/query result value
```

## Workers

Every flow segment is executed by `messenger:consume` workers under supervisord. The worker set
is **derived automatically** from the transport topology — for the configuration above: an events
publisher (relay), receiver + handler pairs for the commands and events queues, a commands
notifier and a query handler. Inspect/override under `messenger_workflow.messenger.workflow`:

```yaml
messenger_workflow:
    messenger:
        workflow:
            worker_defaults:
                supervisor: { autostart: true, autorestart: true, stdout_logfile: /dev/stdout }
            workers:
                - { name: Book store commands handler, type: command_handler, source: book_store_commands, instances: 4, cmd_extra_options: { keepalive: 60 } }
                - { name: Book store events receiver, type: event_receiver, queue: book_store_events, enabled: false }
```

Manual entries merge with the derived set by name or `type|source|queue` identity; `enabled:
false` removes a worker; `labels` carries free-form metadata. `cmd_extra_options.keepalive`
renders `messenger:consume --keepalive=<s>`, protecting a long-running handler on a
competing-consumer source from mid-flight redelivery (keep it below the transport's
`redeliver_timeout`; single-consumer sources ignore the in-flight marker entirely).
`instances > 1` requires a competing-consumer source — on a single-consumer transport the
container fails at compile time, because every process would handle the same messages.

`cmd_extra_options.sleep` renders `messenger:consume --sleep=<s>`, the idle pause between
polls. The derived defaults are already tuned per transport kind and rarely need overriding:
broker-queue workers (receivers, query handlers, no-inbox handlers) get `sleep: 0` because the
AMQP consumer blocks on the broker socket — no polling loop exists to pace. PostgreSQL-backed
workers (publishers, notifiers, inbox handlers) keep the messenger default (1 s), but their
idle pacing really comes from LISTEN/NOTIFY: the worker blocks query-free until a notification
or the `get_notify_timeout` wake-up, so lowering `sleep` there buys nothing. One combination to
avoid: `sleep: 0` on a worker that consumes a PostgreSQL transport **together with** another
transport in one `messenger:consume` — with mixed transports the NOTIFY wait is capped to the
worker's sleep to keep the sibling transport polled, and a zero cap disables the wait entirely,
leaving an unpaced loop of empty polls against the database. Keep the derived
one-transport-per-worker shape and `sleep: 0` stays where it belongs — on AMQP-only workers.

Generate the supervisord config:

```bash
bin/console messenger:supervisor-config                               # stdout
bin/console messenger:supervisor-config --output-dir=etc/supervisor   # one <group>.conf per group
```

## Dedicated queues (routes)

Selected command (or query) classes of a context can be sent to a queue of their own — the
classic case is a **single-consumer FIFO lane** for ordering-sensitive commands next to the
competing regular queue:

```yaml
framework:
    messenger:
        transports:
            book_store_bulk:                               # inbox named after the routed queue
                dsn: 'commands-inbox://book_store?strict_order=true'
                failure_transport: book_store_bulk_failures
            book_store_bulk_failures: 'commands-failures://book_store?queue_name=book_store_bulk'

messenger_workflow:
    messenger:
        transports:
            commands:
                queue_bindings:
                    - { queue: book_store_commands, owner: BookStore }
                    - { queue: book_store_bulk, owner: BookStore, route: bulk,
                        messages: ['App\BookStore\Application\Command\RebuildCatalog'] }
```

Routed classes get the routing key `commands.internal.BookStore.bulk` (or, marked with
`#[MessageRoute('bulk')]`, no configuration entry at all) and reach only `book_store_bulk`;
everything else keeps `commands.internal.BookStore`. The routed queue gets its own derived
receiver and handler (`instances: 1` enforced on a `strict_order` inbox), its own inbox table
(derived when not declared) and DLQ, and publishes tracked results through the context's
existing `book_store_commands_notifier`. Every misconfiguration the bundle can detect fails
the container build. The full walkthrough — routing keys, binding keys, ordering under
failure, validation rules — is in [MESSAGE_FLOWS.md](MESSAGE_FLOWS.md#dedicated-queues--routes).

## Reducing the flow

The optional segments can be removed per context — each removal is an informed trade-off:

| Removed  | Consequence                                                                                                                                                                                                                                                                                                      |
|----------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| outbox   | The bus publishes straight to AMQP. Dispatch and database commit are separate writes (dual-write risk); broker downtime surfaces at dispatch time.                                                                                                                                                               |
| inbox    | The handler worker consumes the broker queue directly (`--queues=<q>`). No deduplication — handlers must be idempotent; per-queue `fromTransport` scoping is unavailable. Map the queue in `orm_mappings` to keep a plain middleware transaction around handlers. The `<queue>_notifier` convention still works. |
| notifier | Tracked command results are written to the result storage directly from the handler worker (a dual write outside the handler transaction). Untracked commands never used the notifier.                                                                                                                           |

All reductions are exercised by the integration suite (`tests/Integration/Flow/ReducedFlowTest.php`)
and walked through hop by hop in [MESSAGE_FLOWS.md](MESSAGE_FLOWS.md#reduced-flows).

## Ordered event delivery

Events are FIFO end-to-end when every segment has a single consumer: the outbox relay is
single-consumer by design, and the events inbox defaults to single-consumer FIFO. A retrying
(poison) message blocks its ordered queue only for its bounded retry budget, then drains to the
failure transport and the queue resumes. Declaring `strict_order=true` on a transport documents
the intent and makes conflicting `multiple_consumers` configuration fail at boot (container
compile time).

Retry backoff delays apply on the inbox hop too: a retry redelivery stamps the row with an
`available_at`. In single-consumer FIFO mode a message in backoff **blocks its successors**
(ordering is preserved — the queue waits, bounded by the flow's total retry budget); in
competing-consumer mode the row is simply skipped until due. On PostgreSQL a worker sleeping
on LISTEN/NOTIFY picks a due retry up at the next idle wake-up — governed by
`get_notify_timeout` (default 60 s) with the idle listener active (the normal bundle mode),
or by the `check_delayed_interval` re-poll (default 60 s) when the transport is wired
standalone without the listener. Lower the matching interval on transports where precise
backoff timing matters.

## The unit of work at the transaction boundary

`WorkflowTransactionMiddleware` opens the transaction around a received message — and
closes the unit of work inside it. Before committing, every **open ORM entity manager
running on that transaction's connection** is flushed, so an ORM application gets a real
unit of work per message:

```php
#[AsCommandHandler]
final readonly class CompleteAuditCommandHandler
{
    public function __invoke(CompleteAuditCommand $command): void
    {
        $audit = $this->audits->get($command->auditId);

        $this->reconciliation->complete($audit, $command->completedBy, $this->clock->now());
        // No flush, no save: the aggregates are managed, and the boundary writes them.
    }
}
```

A handler that flushes itself is unaffected — the boundary flush then finds nothing to do.
The write happens **before the inbox row is removed**, so a failing flush leaves the message
in the inbox and the whole transaction rolls back: the message is retried, never silently
lost.

Managers are selected by **connection name, resolved at container compile time**: the
inbox transport and the entity managers both name their connection in configuration and
resolve the same `doctrine.dbal.<name>_connection` service, so the compiled
`connection name → entity managers` map is exactly the set of managers taking part in
*this* message's transaction. Per message that is a single hash lookup — with dozens of
bounded contexts, the other contexts' managers are never instantiated or scanned. Other
bounded contexts, and managers running their own transactions (a projection writer,
say), are untouched.

Two guards catch the misconfiguration this map cannot repair — handlers writing through
an entity manager on a *different* connection than the message transaction (such writes
could never be atomic with the inbox-row removal):

- **Compile time:** a transactional inbox transport whose connection has no entity
  manager is flagged in the container's compiler log (fine for a DBAL-only context,
  suspicious if that context uses the ORM).
- **Debug mode** (`kernel.debug`): a handler that leaves scheduled (persisted/removed,
  never flushed) entities in a manager outside the transaction fails the message with a
  descriptive `LogicException` instead of silently dropping the writes on commit.

Only *received* messages are wrapped, which is also what makes the flush safe to do here:
a sender-side `dispatch()` carries no `ReceivedStamp` and returns before reaching the
flush — including the outbox's own dispatch, which happens from inside a flush and would
otherwise re-enter one.

Turn it off to leave writing entirely to the application:

```yaml
messenger_workflow:
    messenger:
        transaction:
            flush_entity_managers: false   # default: true
```

## Failure transports (DLQ) and replays

Inbox deduplication covers **broker redelivery only**: a message UUID already recorded as
processed is dropped when RabbitMQ delivers it again. An operator replay
(`messenger:failed:retry`) deliberately **bypasses** that record and re-executes the handler —
that is the escape hatch for the crash window where a message was marked processed but its side
effects were lost (e.g. a non-transactional handler crashing between the application write and
the inbox ack). The operational consequences:

- Handlers behind an inbox must stay **idempotent under operator replay** — replaying an
  already-applied event otherwise re-applies it (a projector may resurrect a deleted row).
- `transactional_handler=true` commits the application writes atomically with the inbox-row
  removal (same DBAL connection), closing the crash window that makes replays necessary — and
  with it most of the double-execution risk.
- Replay order is the failure transport's, not the original queue order — relevant for
  strictly-ordered event streams.

**Why replays do not consult the dedup index (decision, 0.4):** the index marks a message
UUID processed on *both* terminal outcomes — successful ack and permanent rejection — so
every message sitting in a failure transport is already marked processed, and a dedup check
on replay would block **all** replays. More fundamentally, no marker can distinguish "failed
before applying its side effects" from "failed after": whether a replay is safe is a property
of the handler, not of the message's delivery history. Idempotent handlers and
`transactional_handler=true` are the supported mechanisms; the bypass is deliberate and will
stay.

## Testing

The PHPUnit suite (unit + functional + integration) expects live local infrastructure for the
integration groups:

- RabbitMQ v3.13 on `localhost:5672` (management console on `:15672`), user `guest` / password `guest`
- Redis v8.4 on `localhost:6379`, password `xxx` (tests use db 15)
- PostgreSQL v18.4 on `localhost:5432`, user `test` / password `test` (tests skip without `pdo_pgsql`)

```bash
composer install                     # also applies patches/composer/* to the jwage package
vendor/bin/phpunit                   # full suite
vendor/bin/phpunit --testsuite unit  # no I/O
vendor/bin/phpunit --group redis     # infra subsets: redis, rabbitmq, postgres
composer stan                        # PHPStan, max level + strict rules
```

Connection overrides: `MWF_TEST_PG_*`, `MWF_TEST_REDIS_*`, `MWF_TEST_AMQP_DSN`
(defaults in `phpunit.xml.dist`).

## License

This library is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
