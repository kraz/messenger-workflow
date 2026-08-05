<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Infrastructure\Task;

use Kraz\MessengerWorkflow\Application\Messenger\QueryBusInterface;
use Kraz\MessengerWorkflow\Application\Task\TaskOwnerResolverInterface;
use Kraz\MessengerWorkflow\Application\Task\TaskOwnershipRegistryInterface;
use Symfony\Component\Messenger\Envelope;

/**
 * Decorates the query bus so every askAsync() with a resolvable owner is recorded
 * in the ownership registry. Synchronous ask() calls are not recorded — the caller
 * blocks on the answer, there is nothing to poll. See TrackingCommandBus.
 */
final readonly class TrackingQueryBus implements QueryBusInterface
{
    public function __construct(
        private QueryBusInterface $inner,
        private TaskOwnershipRegistryInterface $registry,
        private ?TaskOwnerResolverInterface $ownerResolver = null,
    ) {
    }

    public function ask(object $query, ?int $timeout = null): mixed
    {
        return $this->inner->ask($query, $timeout);
    }

    public function askAsync(object $query): string
    {
        $taskId = $this->inner->askAsync($query);

        $owner = $this->ownerResolver?->resolveOwnerIdentifier();
        if (null !== $owner && '' !== $owner) {
            // record() is best-effort and never throws into the business flow.
            $this->registry->record($taskId, $owner, $query instanceof Envelope ? $query->getMessage()::class : $query::class);
        }

        return $taskId;
    }

    public function await(string $taskId, ?int $timeout = null): mixed
    {
        return $this->inner->await($taskId, $timeout);
    }
}
