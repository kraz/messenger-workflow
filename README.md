# MessengerWorkflow

> [!WARNING]
> This Symfony bundle is a proof of concept. Using it in production is not recommended!

A Symfony bundle implementing Enterprise Integration Patterns on top of Symfony Messenger:
CQRS message buses, transactional outbox/inbox, tracked async tasks and RabbitMQ integration.
It is the messaging backbone for a modular monolith whose bounded contexts (modules) communicate
only asynchronously — so they can later be split into independently deployed applications without
touching application code.

Upgrading from 0.2.x? Read **[UPGRADE-0.3.md](UPGRADE-0.3.md)**.

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
  add ownership records, a status provider (`pending|completed|failed`) and a non-blocking result
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

### Transport DSN options

| Option | Transports | Default | Meaning |
|---|---|---|---|
| `table_name` | outbox, inbox, failures | `zz_commands_*` / `zz_events_*` | storage table (inbox dedup index table = `<table_name>_index`) |
| `multiple_consumers` | inbox | commands `true`, events `false` | competing consumers via `FOR UPDATE SKIP LOCKED` |
| `strict_order` | inbox, outbox | `false` | request single-consumer FIFO; conflicts with `multiple_consumers` (boot-time error) |
| `transactional_handler` | inbox | commands `true`, events `false` | run handlers inside the inbox-row-deleting transaction |
| `redeliver_timeout` | inbox, outbox | `300` | seconds before an unacked in-flight row is redelivered |
| `get_notify_timeout`, `check_delayed_interval` | inbox, outbox (PostgreSQL) | `60000` | LISTEN/NOTIFY wait and re-poll interval (ms) |
| `queue_name` | failures | — | scopes the stock Doctrine failure transport per queue |

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
                - { name: Book store commands handler, type: command_handler, source: book_store_commands, instances: 4 }
                - { name: Book store events receiver, type: event_receiver, queue: book_store_events, enabled: false }
```

Manual entries merge with the derived set by name or `type|source|queue` identity; `enabled:
false` removes a worker; `labels` carries free-form metadata. Generate the supervisord config:

```bash
bin/console messenger:supervisor-config                               # stdout
bin/console messenger:supervisor-config --output-dir=etc/supervisor   # one <group>.conf per group
```

## Reducing the flow

Any `[...]` segment can be removed per context — each removal is an informed trade-off:

| Removed | Consequence |
|---|---|
| outbox | The bus publishes straight to AMQP. Dispatch and database commit are separate writes (dual-write risk); broker downtime surfaces at dispatch time. |
| inbox | The handler worker consumes the broker queue directly (`--queues=<q>`). No deduplication — handlers must be idempotent; per-queue `fromTransport` scoping is unavailable. Map the queue in `orm_mappings` to keep a plain middleware transaction around handlers. The `<queue>_notifier` convention still works. |
| notifier | Tracked command results are written to the result storage directly from the handler worker (a dual write outside the handler transaction). Untracked commands never used the notifier. |

All reductions are exercised by the integration suite (`tests/Integration/Flow/ReducedFlowTest.php`).

## Ordered event delivery

Events are FIFO end-to-end when every segment has a single consumer: the outbox relay is
single-consumer by design, and the events inbox defaults to single-consumer FIFO. A retrying
(poison) message blocks its ordered queue only for its bounded retry budget, then drains to the
failure transport and the queue resumes. Declaring `strict_order=true` on a transport documents
the intent and makes conflicting `multiple_consumers` configuration fail at boot (container
compile time).

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
