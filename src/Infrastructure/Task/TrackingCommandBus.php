<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Infrastructure\Task;

use Kraz\MessengerWorkflow\Application\Messenger\CommandBusInterface;
use Kraz\MessengerWorkflow\Application\Task\TaskOwnerResolverInterface;
use Kraz\MessengerWorkflow\Application\Task\TaskOwnershipRegistryInterface;
use Symfony\Component\Messenger\Envelope;

/**
 * Decorates the command bus so every TRACKED dispatch with a resolvable owner is
 * recorded in the ownership registry — transparently, without controller changes.
 *
 * The by-reference tracking contract is forwarded by ARGUMENT PRESENCE, not by value:
 * dispatch($cmd) stays untracked through the decorator.
 * Dispatches without an owner (workers, CLI — the resolver returns null, or no
 * resolver is registered) are ownerless system tasks and are not recorded.
 */
final readonly class TrackingCommandBus implements CommandBusInterface
{
    public function __construct(
        private CommandBusInterface $inner,
        private TaskOwnershipRegistryInterface $registry,
        private ?TaskOwnerResolverInterface $ownerResolver = null,
    ) {
    }

    public function dispatch(object $command, ?string &$taskId = null): void
    {
        if (\func_num_args() < 2) {
            $this->inner->dispatch($command);

            return;
        }

        $this->inner->dispatch($command, $taskId);

        $owner = $this->ownerResolver?->resolveOwnerIdentifier();
        if (null !== $taskId && null !== $owner && '' !== $owner) {
            // record() is best-effort and never throws into the business flow.
            $this->registry->record($taskId, $owner, $this->messageType($command));
        }
    }

    public function await(string $taskId, ?int $timeout = null): void
    {
        $this->inner->await($taskId, $timeout);
    }

    private function messageType(object $command): string
    {
        return $command instanceof Envelope ? $command->getMessage()::class : $command::class;
    }
}
