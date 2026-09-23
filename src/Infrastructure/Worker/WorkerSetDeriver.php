<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Infrastructure\Worker;

/**
 * Derives the required worker set from the configured transport topology:
 *
 * - outbox transport            → publisher (relay) worker; "*_notifier" outboxes →
 *                                 notifier worker
 * - broker queue + inbox        → receiver worker + handler worker
 * - broker queue without inbox  → single collapsed consumer worker (no-inbox mode)
 * - queries queue               → query handler worker
 *
 * A routed queue (binding with `route`) derives its workers like any other bound
 * queue; its supervisor group is the group of the owner's regular queue, so the
 * dedicated workers stay next to the context's workers.
 *
 * Manually configured workers (workflow.workers) override the derived set: an entry
 * matching a derived worker by NAME or by IDENTITY (type + source + queue) replaces
 * it — old fully-manual configurations therefore produce exactly their declared set.
 * `enabled: false` removes a worker.
 */
final readonly class WorkerSetDeriver
{
    /**
     * @param array<string, string>                    $transportDsns   transport name => DSN
     * @param array<string, array<array-key, mixed>>   $queueBindings   broker transport name (events|commands|queries) => queue bindings (queue => binding)
     * @param list<array<array-key, mixed>>            $manualWorkers   workflow.workers entries (type defaults already applied)
     *
     * @return list<array<array-key, mixed>>
     */
    public function derive(array $transportDsns, array $queueBindings, array $manualWorkers = []): array
    {
        $derived = [];
        foreach ($this->deriveFromTopology($transportDsns, $queueBindings) as $worker) {
            $derived[$this->identity($worker)] = $worker;
        }

        foreach ($manualWorkers as $manual) {
            $identity = $this->identity($manual);
            $matched = null;
            if (isset($derived[$identity])) {
                $matched = $identity;
            } else {
                $manualName = $this->normalizeName($manual['name'] ?? null);
                foreach ($derived as $key => $worker) {
                    if (null !== $manualName && $this->normalizeName($worker['name'] ?? null) === $manualName) {
                        $matched = $key;
                        break;
                    }
                }
            }

            if (null !== $matched) {
                $derived[$matched] = array_replace_recursive($derived[$matched], $manual);
            } else {
                $derived[$this->identity($manual).'#manual'.\count($derived)] = $manual;
            }
        }

        $workers = [];
        foreach ($derived as $worker) {
            if (false === ($worker['enabled'] ?? true)) {
                continue;
            }
            unset($worker['enabled']);
            $workers[] = $worker;
        }

        return $workers;
    }

    /**
     * @param array<string, string>                  $transportDsns
     * @param array<string, array<array-key, mixed>> $queueBindings
     *
     * @return list<array<array-key, mixed>>
     */
    private function deriveFromTopology(array $transportDsns, array $queueBindings): array
    {
        $workers = [];

        foreach ($transportDsns as $name => $dsn) {
            if (!$this->isOutboxDsn($dsn)) {
                continue;
            }
            if (str_ends_with($name, '_notifier')) {
                $workers[] = WorkerTypeDefaults::apply([
                    'name' => $name,
                    'group' => $this->contextOf($name),
                    'type' => 'command_notifier',
                    'source' => $name,
                ]);
            } else {
                $workers[] = WorkerTypeDefaults::apply([
                    'name' => $name.' publisher',
                    'group' => $this->contextOf($name),
                    'type' => 'event_publisher',
                    'source' => $name,
                ]);
            }
        }

        foreach (['commands' => 'command', 'events' => 'event'] as $broker => $kind) {
            $bindings = \is_array($queueBindings[$broker] ?? null) ? $queueBindings[$broker] : [];
            foreach (array_keys($bindings) as $queue) {
                $queue = (string) $queue;
                $group = $this->groupOf($queue, $bindings);
                $hasInbox = $this->isInboxDsn($transportDsns[$queue] ?? '');

                if ($hasInbox) {
                    $workers[] = WorkerTypeDefaults::apply([
                        'name' => $queue.' receiver',
                        'group' => $group,
                        'type' => $kind.'_receiver',
                        'queue' => $queue,
                    ]);
                    $workers[] = WorkerTypeDefaults::apply([
                        'name' => $queue.' handler',
                        'group' => $group,
                        'type' => $kind.'_handler',
                        'source' => $queue,
                    ]);
                } else {
                    // No inbox: the receiver and handler collapse into one worker
                    // consuming the broker queue directly on the handling bus.
                    $worker = WorkerTypeDefaults::apply([
                        'name' => $queue.' handler',
                        'group' => $group,
                        'type' => $kind.'_handler',
                        'source' => $broker,
                        'queue' => $queue,
                    ]);
                    $worker['cmd_extra_options'] = \is_array($worker['cmd_extra_options'] ?? null) ? $worker['cmd_extra_options'] : [];
                    $worker['cmd_extra_options']['sleep'] ??= 0.0;
                    $workers[] = $worker;
                }
            }
        }

        $queryBindings = \is_array($queueBindings['queries'] ?? null) ? $queueBindings['queries'] : [];
        foreach (array_keys($queryBindings) as $queue) {
            $queue = (string) $queue;
            $workers[] = WorkerTypeDefaults::apply([
                'name' => $queue.' handler',
                'group' => $this->groupOf($queue, $queryBindings),
                'type' => 'query_handler',
                'queue' => $queue,
            ]);
        }

        return $workers;
    }

    /**
     * @param array<array-key, mixed> $worker
     */
    private function identity(array $worker): string
    {
        $scalar = static fn (mixed $value): string => \is_scalar($value) ? (string) $value : '';

        return \sprintf('%s|%s|%s', $scalar($worker['type'] ?? null), $scalar($worker['source'] ?? null), $scalar($worker['queue'] ?? null));
    }

    private function normalizeName(mixed $name): ?string
    {
        if (!\is_string($name) || '' === $name) {
            return null;
        }

        $normalized = strtolower((string) preg_replace('/[^a-z0-9]+/i', '-', $name));

        return trim($normalized, '-');
    }

    private function isOutboxDsn(string $dsn): bool
    {
        return str_starts_with($dsn, 'outbox://')
            || str_starts_with($dsn, 'commands-outbox://')
            || str_starts_with($dsn, 'events-outbox://');
    }

    private function isInboxDsn(string $dsn): bool
    {
        return str_starts_with($dsn, 'inbox://')
            || str_starts_with($dsn, 'commands-inbox://')
            || str_starts_with($dsn, 'events-inbox://');
    }

    /**
     * The supervisor group of a bound queue: the context prefix of its name — or, for
     * a routed queue, of the regular queue of the same owner (a routed queue's name
     * carries the route, not a role suffix: "warehouse_planning" belongs with "warehouse").
     *
     * @param array<array-key, mixed> $bindings queue => binding of one broker transport
     */
    private function groupOf(string $queue, array $bindings): string
    {
        $binding = \is_array($bindings[$queue] ?? null) ? $bindings[$queue] : [];
        $route = $binding['route'] ?? null;
        $owner = $binding['owner'] ?? null;
        if (null !== $route && \is_string($owner)) {
            foreach ($bindings as $otherQueue => $otherBinding) {
                if (\is_array($otherBinding) && ($otherBinding['owner'] ?? null) === $owner && null === ($otherBinding['route'] ?? null)) {
                    return $this->contextOf((string) $otherQueue);
                }
            }
        }

        return $this->contextOf($queue);
    }

    /**
     * The bounded-context prefix of a transport/queue name: known role suffixes are
     * stripped ("book_store_commands_notifier" → "book_store").
     */
    private function contextOf(string $name): string
    {
        $context = $name;
        $suffixes = ['_notifier', '_outbox', '_failures', '_commands', '_events', '_queries'];
        do {
            $stripped = false;
            foreach ($suffixes as $suffix) {
                if (str_ends_with($context, $suffix) && \strlen($context) > \strlen($suffix)) {
                    $context = substr($context, 0, -\strlen($suffix));
                    $stripped = true;
                }
            }
        } while ($stripped);

        return $context;
    }
}
