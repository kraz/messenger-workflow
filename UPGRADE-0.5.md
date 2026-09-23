# Upgrade from 0.4.x to 0.5.x

Version 0.5 adds **routes** — dedicated queues for selected command/query classes of a
bounded context — and two compile-time guards. Nothing changes in the message model, bus
names, DSN schemes, Redis key shapes or the supervisor command output. An application
that declares no route gets exactly the 0.4.5 routing keys, bindings and derived workers;
in-flight messages remain readable, no queue drain is required.

---

## 1. New: dedicated queues (`route` on a queue binding)

A queue binding on the `commands` or `queries` broker may declare a `route` and the
message classes it serves; those classes are published with the routing key
`<broker>.[internal.]<Context>.<route>` and reach only that queue. See
[MESSAGE_FLOWS.md — Dedicated queues](MESSAGE_FLOWS.md#dedicated-queues--routes).

```yaml
messenger_workflow:
    messenger:
        transports:
            commands:
                queue_bindings:
                    - { queue: warehouse_commands, owner: Warehouse }
                    - { queue: warehouse_planning, owner: Warehouse, route: planning,
                        messages: ['Warehouse\Feature\Atoms\PlanDocument\PlanDocumentCommand'] }
```

To adopt a route:

1. Declare the routed queue's inbox transport (named after the queue) and, if wanted, its
   failure transport. Do **not** declare a `<queue>_notifier` — the routed queue publishes
   tracked results through the context's existing notifier.
2. Run `bin/console messenger:setup-transports` to provision the queue and its bindings
   (`commands.internal.<Ctx>.<route>` and `commands.<Ctx>.<route>`), and the inbox table.
3. Regenerate the supervisor configuration: a receiver and a handler worker are derived
   for the routed queue.

Deploy order: the routed queue and its workers must exist **before** the new code
dispatches routed messages — a direct exchange drops a message no binding matches. Deploy
the configuration, run `messenger:setup-transports`, start the workers, then release the
dispatching code (or do it all in one deploy with `messenger:setup-transports` before the
workers start, as usual). Messages sitting in a command outbox are routed by the
configuration current when the relay publishes them.

RabbitMQ bindings are additive: renaming or removing a route leaves the old binding on
the queue until you unbind it.

## 2. Guard: inbox/outbox transports must not share a storage table

The workflow Doctrine transports default their table per DSN scheme (`zz_commands_inbox`,
`zz_commands_outbox`, `zz_events_inbox`, `zz_events_outbox`). Two inbox transports — or
two outbox transports — on one DBAL connection without an explicit `table_name` therefore
shared one table: both workers consumed the same rows, one dedup index served both, and
nothing failed. The container build now fails for such a pair, naming both transports and
the table.

If your build fails after upgrading, the configuration was already broken: give all but
one of the named transports an explicit `table_name` (`?table_name=...` in the DSN or
`options.table_name`). The common shape is a `commands-outbox://` command outbox next to a
`commands-outbox://` notifier — the README has always declared the notifier with
`?table_name=zz_commands_notifier`. Same for an explicit `index_table_name` shared by two
transports. Failure transports are not affected (they filter by `queue_name` and share
their table by design).

## 3. Guard: one unrouted queue per owner on a direct exchange

Two `queue_bindings` entries with the same `owner` and no `route` on the `commands` or
`queries` transport bind both queues to the same keys, so every message was delivered to
both. The container build now fails for that. Use a `route` for the queue that must
receive only selected classes; the topic exchange (events) is unaffected — several event
queues per context remain legitimate.

## 4. For anyone wiring the internals programmatically

The bundle does all of this itself; the changes only matter if you construct these
services by hand:

- `AmqpStampFactory::__construct()` takes an optional `MessageRouteResolver` (the compiled
  `message class → route` map; an empty resolver routes nothing).
- `CommandNotifierMiddleware::__construct()` gained an optional fourth parameter, the
  compiled `queue → notifier transport` map (`array<string, string|null>`) consulted before
  the `<receiver>_notifier` / `<queue>_notifier` convention.
- `RoutingKey::withRoute()` and `RoutingKey::ROUTE_PATTERN` are new; the attribute
  `Kraz\MessengerWorkflow\Application\Attribute\MessageRoute` is new.
- `ConfigureTransportsPass` now also sets `table_name` on the definition of a routed inbox
  transport declared without one (`<scheme default>_<transport name>`).
