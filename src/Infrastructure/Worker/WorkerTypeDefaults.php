<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Infrastructure\Worker;

/**
 * Per-type worker defaults. The `type` taxonomy of the original package is
 * kept; the default target buses map to the rewrite's architecture:
 *
 * | type             | consumes (source)      | dispatches on (target) |
 * |------------------|------------------------|------------------------|
 * | event_publisher  | <events outbox>        | relay.bus              |
 * | event_receiver   | events (broker queue)  | inbox.bus              |
 * | event_handler    | <events inbox>         | event.bus              |
 * | command_receiver | commands (broker queue)| inbox.bus              |
 * | command_handler  | <commands inbox>       | command.bus            |
 * | command_notifier | <notifier outbox>      | notifier.bus           |
 * | query_handler    | queries (broker queue) | query.bus              |
 */
final readonly class WorkerTypeDefaults
{
    public const array TYPES = [
        'event_publisher',
        'event_receiver',
        'event_handler',
        'command_receiver',
        'command_handler',
        'command_notifier',
        'query_handler',
    ];

    /**
     * Applies the per-type source/target/sleep defaults to a worker configuration.
     *
     * @param array<array-key, mixed> $worker
     *
     * @return array<array-key, mixed>
     */
    public static function apply(array $worker): array
    {
        $type = $worker['type'] ?? null;
        if (null === $type) {
            return $worker;
        }

        $withSleepDefault = static function (array $worker): array {
            $options = \is_array($worker['cmd_extra_options'] ?? null) ? $worker['cmd_extra_options'] : [];
            $options['sleep'] ??= 0.0;
            $worker['cmd_extra_options'] = $options;

            return $worker;
        };

        switch ($type) {
            case 'event_publisher':
                $worker['target'] ??= 'relay.bus';
                break;
            case 'event_receiver':
                $worker['source'] ??= 'events';
                $worker['target'] ??= 'inbox.bus';
                $worker = $withSleepDefault($worker);
                break;
            case 'event_handler':
                $worker['target'] ??= 'event.bus';
                break;
            case 'command_receiver':
                $worker['source'] ??= 'commands';
                $worker['target'] ??= 'inbox.bus';
                $worker = $withSleepDefault($worker);
                break;
            case 'command_handler':
                $worker['target'] ??= 'command.bus';
                break;
            case 'command_notifier':
                $worker['target'] ??= 'notifier.bus';
                break;
            case 'query_handler':
                $worker['source'] ??= 'queries';
                $worker['target'] ??= 'query.bus';
                $worker = $withSleepDefault($worker);
                break;
            default:
                throw new \LogicException(\sprintf('Unknown worker type: "%s"', \is_scalar($type) ? (string) $type : get_debug_type($type)));
        }

        return $worker;
    }
}
