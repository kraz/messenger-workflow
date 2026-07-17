# MessengerWorkflow

> [!WARNING]
> This Symfony bundle is a proof of concept. Using it in production is not recommended!

A Symfony bundle implementing Enterprise Integration Patterns on top of Symfony Messenger:
CQRS message buses, transactional outbox/inbox, tracked async tasks and RabbitMQ integration.
It is the messaging backbone for a modular monolith whose bounded contexts (modules) communicate
only asynchronously — so they can later be split into independently deployed applications without
touching application code.

## Message model

- **Commands** — change state, no return value, fire-and-forget. Exactly one handler.
- **Tasks** — commands that return a value; tracked by UUID, awaitable/pollable. Exactly one handler.
- **Queries** — return a result, no side effects. Exactly one handler.
- **Domain events** — immutable notifications about domain changes. Zero or more handlers.

## Features

### CQRS buses
- Five preconfigured Messenger buses: `command.bus`, `query.bus`, `event.bus` and the internal
  `outbox.bus` / `inbox.bus` (not used directly by application code).
- Type validation in middleware (and in the `CommandBusInterface` / `QueryBusInterface` /
  `EventBusInterface` wrapper services) prevents dispatching the wrong message type on the wrong bus.
- The exactly-one-handler rule for commands and queries is enforced at dispatch time — it can not be
  checked at compile time, because the handler may live in a separately deployed bounded context.
- Handlers are registered with the `#[AsCommandHandler]` / `#[AsQueryHandler]` / `#[AsEventHandler]`
  attributes (class or method level, with optional `fromTransport`).
- Commands and queries with a local handler execute synchronously in-process; everything else is
  published to the broker.

### Tasks
- Dispatching a task returns a globally unique ID (UUID v7).
- Results and errors are stored in the configured result storage — `redis` provider for production,
  in-memory by default — and are retrievable by UUID. No intermediate statuses are tracked: a task
  is observable as pending (no result yet), completed (result available) or failed (error available).
- Sync mode awaits completion; async mode polls by UUID.
- The task result notification is submitted via the outbox, atomically with the handler's DB
  transaction, for guaranteed delivery.
- The in-memory result storage is single-process: awaiting a missing result throws immediately.
  Remote/async awaits require the `redis` provider.

### Transactional outbox
- Domain events registered from inside the model are published via the outbox — stored atomically
  with the domain data in one DB transaction, "at least once" delivery even if the broker is down.
- FIFO ordering preserved (ORDER BY auto-increment id); a publisher worker relays outbox messages
  to RabbitMQ.
- Domain events can also be published directly to the event bus for immediate effect, at the cost
  of resilience.

### Inbox with deduplication
- Commands/tasks use the inbox by default (configurable per queue): a receiver worker relays broker
  messages into the inbox, a handler worker processes them.
- Deduplication by message UUID covers every attempted message: after a permanent failure the
  message is kept in the failure transport for operator retry, while broker redeliveries of the
  same UUID are silently dropped — a UUID is handled at most once, even when its handling failed.
- Queries bypass the inbox for better performance.

### RabbitMQ integration
- Built on `jwage/phpamqplib-messenger` for `basic_consume` performance.
- Direct exchanges for commands/queries, topic exchange for events; routing keys are derived from
  the message namespace (public `Contracts\*` contracts vs `internal.<Module>` messages).
- No `auto_setup` on the AMQP transports — the broker topology is provisioned externally
  (e.g. by Docker).

### Doctrine transports
- Dedicated outbox / inbox / failure transport factories per bounded context; the DSN host maps to
  the Doctrine DBAL connection name and each context owns its DB schema.
- PostgreSQL `LISTEN/NOTIFY` wakes idle workers instead of polling; multiple competing consumers
  are supported via `FOR UPDATE SKIP LOCKED` with a redelivery timeout.

### Worker management
- 7 worker types: `event_publisher`, `event_receiver`, `event_handler`, `command_receiver`,
  `command_handler`, `command_notifier`, `query_handler`.
- The `messenger:supervisor-config` console command generates the supervisord program/group
  configuration from the bundle's worker configuration.

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
```

Connection overrides: `MWF_TEST_PG_*`, `MWF_TEST_REDIS_*`, `MWF_TEST_AMQP_DSN`
(defaults in `phpunit.xml.dist`).

## License

This library is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
