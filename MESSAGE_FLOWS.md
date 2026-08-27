# Message Flow walkthroughs — end to end

Companion to the [README](README.md): every flow configuration the bundle supports, traced
hop by hop — which process executes each segment, where the message physically sits between
hops, and which configuration line created the segment. Read this when deciding which
segments a bounded context needs, or when you want to know exactly what happens after a
`dispatch()`.

All examples use a bounded context **BookStore** with its own database (Doctrine DBAL
connection `book_store`) and, where a second context is needed, a subscribing context
**Billing**.

- [The moving parts](#the-moving-parts)
  - [Buses](#buses)
  - [How a message finds its transport](#how-a-message-finds-its-transport)
  - [How a message finds its broker queue](#how-a-message-finds-its-broker-queue)
  - [Workers](#workers)
- [Command flow — full (outbox + inbox + notifier)](#command-flow--full-outbox--inbox--notifier)
- [Event flow — full (outbox + inbox)](#event-flow--full-outbox--inbox)
- [Query flow](#query-flow)
- [Reduced flows](#reduced-flows)
  - [Command without outbox (the default dispatch)](#command-without-outbox-the-default-dispatch)
  - [Without inbox — direct queue consumption](#without-inbox--direct-queue-consumption)
  - [Without notifier](#without-notifier)
  - [Minimal — broker only](#minimal--broker-only)
- [Custom transports and queues](#custom-transports-and-queues)

## The moving parts

### Buses

The bundle prepends six Messenger buses. Three are application-facing, three are internal —
consumed only by workers to move messages between segments:

| Bus            | Facing      | Entered via                                          | Key middleware (in order)                                          |
|----------------|-------------|------------------------------------------------------|--------------------------------------------------------------------|
| `command.bus`  | application | `CommandBusInterface::dispatch()`                    | message id → AMQP routing → transaction → notifier → exactly-one-handler |
| `query.bus`    | application | `QueryBusInterface::ask()` / `askAsync()`            | message id → AMQP routing → query result → exactly-one-handler     |
| `event.bus`    | application | `EventBusInterface::publish()`, `OutboxBusInterface` | message id → AMQP routing → transaction                            |
| `relay.bus`    | internal    | outbox **publisher** workers                         | outbox relay (outbox row → broker)                                  |
| `inbox.bus`    | internal    | **receiver** workers                                 | inbox relay (broker delivery → inbox row)                           |
| `notifier.bus` | internal    | **notifier** workers                                 | result notifier (notification → result storage)                     |

Middleware on the application buses is send-side *and* receive-side: the same
`command.bus` that routes your dispatch to the broker also runs in the handler worker,
where the transaction/notifier/handler middleware do their work (they gate on the
`ReceivedStamp`, so nothing handler-related runs at dispatch time).

### How a message finds its transport

1. **Marker-interface routing (the default).** The bundle prepends

   ```yaml
   framework:
       messenger:
           routing:
               'Kraz\MessengerWorkflow\Domain\DomainEventInterface':      events
               'Kraz\MessengerWorkflow\Application\CommandInterface':     commands
               'Kraz\MessengerWorkflow\Application\QueryInterface':       queries
   ```

   so with no further configuration every dispatch goes **straight to the AMQP broker
   transport** of its kind. That is the no-outbox flow — the outbox is the opt-in, not
   the default.

2. **`TransportNamesStamp` (the per-dispatch override).** A `TransportNamesStamp` on the
   envelope *replaces* the routed senders entirely. This is how a message enters an
   outbox — `OutboxBus::publish()` does exactly this internally, and commands opt in the
   same way:

   ```php
   use Symfony\Component\Messenger\Envelope;
   use Symfony\Component\Messenger\Stamp\TransportNamesStamp;

   $commandBus->dispatch(
       Envelope::wrap($command)->with(new TransportNamesStamp(['book_store_commands_outbox'])),
       $taskId,
   );
   ```

   It also sends to *any other* transport you configured — see
   [Custom transports and queues](#custom-transports-and-queues).

   > **Caveat — per-class `framework.messenger.routing` entries:** Symfony's senders
   > locator matches the message class *and* its interfaces, and the marker-interface
   > route above is always present. Routing `App\...\MyCommand: my_transport` therefore
   > sends the message to **both** `my_transport` and `commands`. Use the stamp when you
   > want exclusive redirection.

### How a message finds its broker queue

Senders only pick the AMQP **exchange** (`commands`/`queries` are direct exchanges,
`events` is a topic exchange). Which **queue** receives the message is decided by
RabbitMQ, matching the message's *routing key* against the queues' *binding keys*.

**Routing keys** are derived from the message FQCN by `AmqpRoutingMiddleware` /
`AmqpStampFactory`. Classes under a `Contracts\` root are *public contract* messages;
everything else is *internal* to its context:

| Message class                              | Exchange type | Routing key                                     |
|--------------------------------------------|---------------|--------------------------------------------------|
| `App\BookStore\Application\Command\RegisterBook` | direct  | `commands.internal.BookStore`                    |
| `Contracts\BookStore\Command\RegisterBook` | direct        | `commands.BookStore`                             |
| `App\BookStore\Domain\Event\BookRegistered`| topic         | `events.internal.BookStore.Domain.Event.BookRegistered` |
| `Contracts\BookStore\Event\BookRegistered` | topic         | `events.BookStore.Event.BookRegistered`          |

Direct exchanges route on the bounded-context segment only; the topic exchange keeps the
full dotted path so subscribers can bind selectively. The middleware only stamps when the
envelope carries no `AmqpStamp` yet — pre-stamping your own `AmqpStamp` gives you full
control of the routing key.

**Binding keys** come from `messenger_workflow.messenger.transports.<broker>.queue_bindings`.
Each entry declares a queue and its owning context; `ConfigureTransportsPass` computes the
keys and writes them into the transport's `options.queues.<queue>.binding_keys`
(provisioned by `messenger:setup-transports`):

```yaml
messenger_workflow:
    messenger:
        transports:
            commands:
                queue_bindings:
                    - { queue: book_store_commands, owner: BookStore }
            events:
                queue_bindings:
                    - { queue: book_store_events, owner: BookStore }
```

derives:

| Queue                 | Binding keys                                                                                                         |
|-----------------------|----------------------------------------------------------------------------------------------------------------------|
| `book_store_commands` | `commands.BookStore` (public), `commands.internal.BookStore` (internal)                                               |
| `book_store_events`   | `events.internal.BookStore.#` (all own internal events) + one key per foreign event class handled with `#[AsEventHandler(fromTransport: 'book_store_events')]` + any explicit `binding_keys: [...]` |

On the topic exchange the foreign-event keys are auto-derived from the handler
declarations — a Billing handler for `Contracts\BookStore\Event\BookRegistered` bound to
`billing_events` automatically adds the binding key
`events.BookStore.Event.BookRegistered` to the `billing_events` queue. Explicit
`binding_keys` entries (class names or ready-made dotted keys, wildcards allowed) cover
the cases derivation cannot see.

### Workers

Every segment between two storages is executed by a `messenger:consume` process. The
worker set is derived from the transport topology (`WorkerSetDeriver`) and rendered by
`messenger:supervisor-config`:

| Worker type        | Derived when...                              | Consumes (source)     | Rendered command                                                              |
|--------------------|----------------------------------------------|------------------------|--------------------------------------------------------------------------------|
| `event_publisher`  | any outbox transport not ending `_notifier`  | the outbox transport   | `messenger:consume --bus=relay.bus <outbox>`                                    |
| `command_receiver` | commands queue **with** an inbox             | broker `commands`      | `messenger:consume --bus=inbox.bus --queues=<queue> --sleep=0 commands`         |
| `command_handler`  | commands queue with an inbox                 | the inbox transport    | `messenger:consume --bus=command.bus <queue>`                                   |
| `command_handler` (collapsed) | commands queue **without** an inbox | broker `commands`   | `messenger:consume --bus=command.bus --queues=<queue> --sleep=0 commands`       |
| `command_notifier` | outbox transport ending `_notifier`          | the notifier outbox    | `messenger:consume --bus=notifier.bus <queue>_notifier`                         |
| `event_receiver`   | events queue with an inbox                   | broker `events`        | `messenger:consume --bus=inbox.bus --queues=<queue> --sleep=0 events`           |
| `event_handler`    | events queue with an inbox                   | the inbox transport    | `messenger:consume --bus=event.bus <queue>`                                     |
| `event_handler` (collapsed) | events queue without an inbox       | broker `events`        | `messenger:consume --bus=event.bus --queues=<queue> --sleep=0 events`           |
| `query_handler`    | every queries queue                          | broker `queries`       | `messenger:consume --bus=query.bus --queues=<queue> --sleep=0 queries`          |

(The `event_publisher` type also serves command outboxes — a publisher relays whatever
kind of outbox it consumes.) Manual `workflow.workers` entries merge with the derived set
by name or `type|source|queue` identity; `enabled: false` removes a worker.

## Command flow — full (outbox + inbox + notifier)

**Every optional segment present.** Strongest guarantees, most infrastructure.

```yaml
framework:
    messenger:
        transports:
            commands: { dsn: '%env(MSG_BROKER_DSN)%' }

            # host = Doctrine DBAL connection name of the context's database
            book_store_commands_outbox: 'commands-outbox://book_store'
            book_store_commands:                              # inbox — named after the broker queue!
                dsn: 'commands-inbox://book_store'
                failure_transport: book_store_commands_failures
            book_store_commands_failures: 'commands-failures://book_store?queue_name=book_store_commands'
            book_store_commands_notifier: 'commands-outbox://book_store?table_name=zz_commands_notifier'

messenger_workflow:
    messenger:
        transports:
            commands:
                queue_bindings:
                    - { queue: book_store_commands, owner: BookStore }
        result_storage: { provider: redis, service: redis_client.default }
```

Derived workers: `book_store_commands_outbox publisher`, `book_store_commands receiver`,
`book_store_commands handler`, `book_store_commands_notifier`.

```php
$this->commandBus->dispatch(
    Envelope::wrap(new RegisterBook('978-3-16'))
        ->with(new TransportNamesStamp(['book_store_commands_outbox'])),
    $taskId,                       // presence of the argument = tracked
);
// ... later, anywhere:
$this->commandBus->await($taskId, timeout: 30);
```

The journey, hop by hop:

1. **Dispatch** *(caller's process, e.g. an HTTP request)* — `CommandBus` verifies the
   message implements `CommandInterface`, stamps a `MessageIdStamp` (UUID v7 — this **is**
   `$taskId`) and, because the `$taskId` argument is present, a `ResultTrackedStamp`.
   `AmqpRoutingMiddleware` attaches the routing key (`commands.internal.BookStore`) and
   the AMQP `message_id` attribute. The `TransportNamesStamp` overrides the routed
   senders, so `SendMessageMiddleware` **inserts one row into `zz_commands_outbox`** on
   the `book_store` connection — and nothing else. If the caller has an open transaction
   on that connection, the row commits **atomically with the domain writes**: that is the
   transactional-outbox guarantee. Dispatch returns immediately.

2. **Outbox → broker** *(publisher worker: `messenger:consume --bus=relay.bus
   book_store_commands_outbox`)* — the outbox transport is single-consumer FIFO by
   auto-increment id; on PostgreSQL, `LISTEN/NOTIFY` wakes an idle relay instantly.
   `OutboxRelayMiddleware` rebuilds a fresh envelope from the message plus its
   *transferable* stamps (message id, tracking, ordering), re-attaches the AMQP stamp and
   publishes to the `commands` direct exchange. Success deletes the row; a publish
   failure keeps it (outbox transports run with `max_retries=0`, so a failure rejects
   back into the outbox with `retry_count` incremented — at-least-once even while the
   broker is down).

3. **Broker** — RabbitMQ matches routing key `commands.internal.BookStore` against the
   binding keys of `book_store_commands` and enqueues there.

4. **Broker → inbox** *(receiver worker: `messenger:consume --bus=inbox.bus
   --queues=book_store_commands --sleep=0 commands`)* — `InboxRelayMiddleware` moves the
   delivery into the **inbox transport with the same name as the queue** (that naming
   convention is load-bearing) — an `INSERT` into `zz_commands_inbox` plus a dedup-index
   row keyed by the message UUID, then acks the broker. A broker **redelivery of an
   already-recorded UUID is dropped** here — at-most-once handling. A storage failure
   nacks, so the broker redelivers: the hand-off is at-least-once, the dedup makes it
   effectively exactly-once into the inbox.

5. **Handling** *(handler worker: `messenger:consume --bus=command.bus
   book_store_commands`)* — commands inboxes default to competing consumers
   (`FOR UPDATE SKIP LOCKED`, so `instances > 1` is fine) and
   `transactional_handler=true`. `WorkflowTransactionMiddleware` opens a transaction on
   the `book_store` connection; `CommandNotifierMiddleware` resolves the
   `book_store_commands_notifier` outbox (by the `<receiving transport>_notifier`
   convention) *before* handling — a misconfigured notifier fails the message without
   side effects; `ExactlyOneHandlerMiddleware` enforces exactly one handler. Then, inside
   the one transaction: the handler runs (application writes on the same connection join
   it), the handler's return value is wrapped into a `CommandCompletedNotification` and
   inserted into the notifier outbox, the entity managers on that connection are flushed
   (the unit of work the handler did not have to close itself — disable with
   `messenger_workflow.messenger.transaction.flush_entity_managers: false`), the inbox row
   is deleted and its dedup entry marked processed — **one atomic commit** for all of it.

   On failure everything rolls back and the command retry policy decides: transient
   infrastructure errors retry with backoff (the delay is applied as `available_at` on
   the inbox row), everything else — or an exhausted budget — drains the message to
   `book_store_commands_failures` (DLQ, replayable with `messenger:failed:retry`).

6. **Notifier → result storage** *(notifier worker: `messenger:consume --bus=notifier.bus
   book_store_commands_notifier`)* — `ResultNotifierMiddleware` writes the result value
   under `rs:[<ns>:]<taskId>` in Redis. Because the notification sat in an outbox that
   committed with the handler transaction, the result publication inherits the
   at-least-once guarantee.

7. **Await** *(caller)* — `await($taskId)` blocks on the result storage; `TaskFailedException`
   carries a permanent failure, `TaskTimeOutException` a timeout. Awaiting a task whose
   outbox row still sits in *your own uncommitted transaction* throws
   `PendingOutboxMessageException` immediately instead of deadlocking. With
   `tasks.enabled: true` you can instead poll `TaskStatusProviderInterface` /
   `TaskResultProviderInterface` (e.g. from an HTTP status endpoint).

**Untracked variant:** omit the `$taskId` argument — no `ResultTrackedStamp`, steps 6–7
disappear, the notifier outbox is never touched. Same reliability for the state change
itself.

## Event flow — full (outbox + inbox)

```yaml
framework:
    messenger:
        transports:
            events: { dsn: '%env(MSG_BROKER_DSN)%' }

            # publishing side (BookStore)
            book_store_outbox: 'events-outbox://book_store'

            # consuming side (each subscribing context; here: Billing and BookStore itself)
            billing_events:
                dsn: 'events-inbox://billing'
                failure_transport: billing_events_failures
            billing_events_failures: 'events-failures://billing?queue_name=billing_events'

messenger_workflow:
    messenger:
        transports:
            events:
                queue_bindings:
                    - { queue: billing_events, owner: Billing }
        outbox_buses:
            book_store: book_store_outbox     # registers OutboxBusInterface $bookStoreOutboxBus
```

```php
// inside a domain service of BookStore, same transaction as the aggregate writes:
$this->bookStoreOutboxBus->publish(new BookRegistered($bookId));   // Contracts\BookStore\Event\BookRegistered
```

```php
// in Billing:
#[AsEventHandler(fromTransport: 'billing_events')]
final class BookRegisteredHandler
{
    public function __invoke(BookRegistered $event): void { /* ... */ }
}
```

1. **Publish** *(BookStore process)* — `OutboxBus` wraps the event with
   `TransportNamesStamp(['book_store_outbox'])` and dispatches on `event.bus`: one row in
   `zz_events_outbox`, atomic with the domain transaction. (Publishing via
   `EventBusInterface` instead skips the outbox — immediate but dual-write, see
   [reduced flows](#reduced-flows).)

2. **Outbox → broker** *(publisher worker)* — identical to the command flow, but onto the
   `events` **topic** exchange with the full dotted routing key
   `events.BookStore.Event.BookRegistered`. The outbox relay is single-consumer FIFO, so
   publication order is the outbox insertion order.

3. **Fan-out** *(broker)* — the topic exchange copies the message into **every queue whose
   binding keys match**: `billing_events` matches because Billing declares a handler with
   `fromTransport: 'billing_events'` for that class (auto-derived binding key
   `events.BookStore.Event.BookRegistered`); `book_store_events`, if configured, would
   match its own `events.internal.BookStore.#` wildcard for internal events. Contexts
   with no matching binding never see the message. Each queue now owns an independent
   copy — one context failing affects nobody else.

4. **Broker → inbox** *(receiver worker per context)* — as in the command flow: dedup by
   message UUID into `zz_events_inbox` on the *consuming* context's database.

5. **Handling** *(handler worker per context: `messenger:consume --bus=event.bus
   billing_events`)* — events inboxes default to **single-consumer FIFO** (ordered) and
   `transactional_handler=false` (opt in per transport when handlers write to the same
   database). Zero to N handlers run; `fromTransport` scoping ensures only the handlers
   declared for this queue fire. The event retry policy always retries (backoff + jitter)
   within a bounded total budget, then drains to the DLQ — a poison message can block an
   ordered queue only for its bounded retry budget.

   **Ordering:** events are FIFO end-to-end when every segment has a single consumer —
   outbox relay (always) → queue (one receiver) → inbox in FIFO mode (default). Declare
   `strict_order: true` on the transports to make conflicting `multiple_consumers`
   configuration fail at container compile time. In FIFO mode a message in retry backoff
   *blocks its successors* (deliberately — order is preserved); in competing-consumer
   mode the delayed row is simply skipped until due.

## Query flow

Queries are always "reduced" **by design**: no outbox (a query changes no state — there is
nothing to keep atomic), no inbox (deduplication is pointless for idempotent reads), no
notifier (the result storage is the return channel). Their entire flow:

```yaml
framework:
    messenger:
        transports:
            queries: { dsn: '%env(MSG_BROKER_DSN)%' }

messenger_workflow:
    messenger:
        transports:
            queries:
                queue_bindings:
                    - { queue: book_store_queries, owner: BookStore }
```

Derived worker: `book_store_queries handler` —
`messenger:consume --bus=query.bus --queues=book_store_queries --sleep=0 queries`.

1. **Ask** *(caller)* — `ask()` is `askAsync()` + `await()`. `askAsync()` stamps the
   message id (= task id) and a `ResultTrackedStamp` — queries are *always* tracked — and
   routes to the `queries` direct exchange (routing key `queries.internal.BookStore` /
   `queries.BookStore`).

2. **Handling** *(query handler worker)* — consumes the broker queue directly on
   `query.bus`. `ExactlyOneHandlerMiddleware` enforces one handler;
   `QueryResultMiddleware` writes the handler's return value to the result storage. No
   transaction is opened (map the queue in `orm_mappings` if you need one).

3. **Retries and failure** — the aggressive query retry policy (fast, transient-only) is
   attached to the `queries` broker transport itself; retry delays run through the
   `queries.delay` exchange. There is **no failure transport**: a permanent failure is
   written to the result storage as an error — the awaiting caller gets a
   `TaskFailedException` — and the message is dropped. A query result nobody can await
   has no value; parking it in a DLQ would only hide the error from the asker.

4. **Await** *(caller)* — `await($taskId)` returns the value (or throws). The stored
   result expires after `result_storage.expire_input_after` (default 3 h).

## Reduced flows

Each removal below is an informed trade-off, not a degraded mode. All variants are
exercised by `tests/Integration/Flow/ReducedFlowTest.php` (and
`CommandFlowTest`/`EventFlowTest` for the full and no-outbox rows).

### Command without outbox (the default dispatch)

**Config:** simply don't add an outbox transport — and dispatch without a
`TransportNamesStamp`:

```php
$this->commandBus->dispatch(new RegisterBook('978-3-16'), $taskId);
```

The marker-interface routing sends the command **straight to the `commands` exchange**
from the caller's process. Steps 2 of the full flow disappears; steps 3–7 are unchanged.
Events behave the same when published through `EventBusInterface` instead of an
`OutboxBus`.

**Trade-off:** dispatch and database commit are two separate writes. A crash between
them loses one of the two (dual-write risk), and broker downtime surfaces as an exception
at dispatch time instead of being absorbed by the outbox. Use it for messages that don't
accompany database writes — or when the caller has no database at all.

### Without inbox — direct queue consumption

**Config:** declare the broker queue, but no transport named after it:

```yaml
framework:
    messenger:
        transports:
            commands: { dsn: '%env(MSG_BROKER_DSN)%' }
            book_store_commands_notifier: 'commands-outbox://book_store?table_name=zz_commands_notifier'
            # NO "book_store_commands" inbox transport

messenger_workflow:
    messenger:
        transports:
            commands:
                queue_bindings:
                    - { queue: book_store_commands, owner: BookStore }
                orm_mappings:
                    book_store_commands: book_store    # queue → entity manager (or DBAL connection) name
```

The worker derivation notices the missing inbox and **collapses receiver + handler into
one worker** consuming the broker queue directly on the handling bus:

```
messenger:consume --bus=command.bus --queues=book_store_commands --sleep=0 commands
```

What changes in the journey:

- Steps 4–5 of the full flow merge: handlers run directly on the broker delivery. The
  flow's retry policy moves onto the **broker transport itself** (delays through the
  AMQP delay exchange) — `ConfigureTransportsPass` wires this automatically when no
  inbox of that kind exists.
- **No deduplication.** The broker's at-least-once delivery reaches your handlers —
  they must be idempotent.
- **No transaction with the ack.** The `orm_mappings` entry above gives handlers a plain
  transaction on the mapped connection (commit on success, rollback on failure), but the
  broker ack happens *outside* it. Without a mapping, no transaction is opened at all.
- **No `fromTransport` scoping** for this queue (there is no inbox transport name for
  handlers to bind to).
- The **`<queue>_notifier` convention still works**: `CommandNotifierMiddleware` falls
  back to the AMQP queue name to resolve `book_store_commands_notifier`, so tracked
  results keep their outbox guarantee.

The event variant is identical with `events`/`event.bus` — the ReducedFlowTest "Mini"
context runs both.

### Without notifier

**Config:** full flow, minus the `<queue>_notifier` transport.

When `CommandNotifierMiddleware` finds no notifier outbox for the receiving transport (or
queue), it writes the tracked result **directly to the result storage from the handler
worker** — a dual write outside the handler transaction. The crash window: handler
transaction committed, Redis write lost → the command *is applied* but the awaiting
caller times out. Untracked commands never used the notifier, so removing it costs them
nothing.

### Minimal — broker only

No outbox, no inbox, no notifier, no orm mapping — nothing but the broker queue (the
"Nano" context in `ReducedFlowTest`):

```yaml
framework:
    messenger:
        transports:
            commands: { dsn: '%env(MSG_BROKER_DSN)%' }

messenger_workflow:
    messenger:
        transports:
            commands:
                queue_bindings:
                    - { queue: nano_commands, owner: Nano }
```

One derived worker: `messenger:consume --bus=command.bus --queues=nano_commands --sleep=0
commands`. Dispatch goes straight to the broker; the handler runs without transaction or
dedup; a tracked result is written directly to the result storage. This is at-least-once,
idempotency-is-your-problem messaging — the right shape for cheap, repeatable operations
(cache warmups, notifications, projections that overwrite), and the wrong one for
"charge the customer".

## Custom transports and queues

The bundle's transports are ordinary `framework.messenger.transports` entries — you can
add your own and steer specific messages onto them without touching the default flows.

**A custom transport, per dispatch.** Any transport you configure (AMQP, Doctrine,
Redis, ...) is reachable with a `TransportNamesStamp` — it replaces the routed senders
for that one dispatch:

```yaml
framework:
    messenger:
        transports:
            bulk_import_commands: '%env(MSG_BROKER_DSN)%'   # or doctrine://, redis://, ...
```

```php
$commandBus->dispatch(
    Envelope::wrap(new ImportBooks($batch))->with(new TransportNamesStamp(['bulk_import_commands'])),
);
```

The workflow middleware still stamps the message id and (on AMQP transports) the routing
key, so tracked results and dedup keep working wherever the flow re-joins the standard
segments.

**A custom queue on a broker exchange.** Add a `queue_bindings` entry with explicit
binding keys — for example a slow-lane queue for one heavy command, consumed by its own
worker so it never starves the context's main queue:

```yaml
messenger_workflow:
    messenger:
        transports:
            commands:
                queue_bindings:
                    - { queue: book_store_commands, owner: BookStore }
                    # a distinct owner segment gives the queue its own binding keys
                    # (commands.BookStoreSlow + commands.internal.BookStoreSlow) —
                    # reusing "BookStore" would double-deliver with the main queue
                    - { queue: book_store_commands_slow, owner: BookStoreSlow }
        workflow:
            workers:
                - { name: Slow commands, type: command_handler,
                    source: commands, queue: book_store_commands_slow, instances: 1 }
```

and route the heavy messages there with a pre-stamped routing key (the AMQP routing
middleware never overwrites an existing `AmqpStamp`):

```php
use Jwage\PhpAmqpLibMessengerBundle\Transport\AmqpStamp;

$commandBus->dispatch(
    Envelope::wrap(new RebuildCatalog())->with(new AmqpStamp('commands.internal.BookStoreSlow')),
);
```

`messenger:setup-transports` provisions the new queue and bindings; the queue can of
course also get its own inbox (name the inbox transport `book_store_commands_slow`) to
re-gain dedup and transactional handling.

**What to remember:**

- `TransportNamesStamp` picks the *transport* (exclusive, per dispatch).
- `AmqpStamp` picks the *routing key* — and therefore the queue(s) — on the broker.
- `queue_bindings` declares the queue and what it listens to; `orm_mappings` restores a
  handler transaction where no inbox exists; the `<queue>_notifier` convention restores
  tracked-result outboxing.
- Per-class `framework.messenger.routing` entries **add** senders (interface routing
  stays active) — prefer the stamp for exclusive redirection.
