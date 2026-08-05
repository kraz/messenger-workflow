<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Application\Task;

/**
 * Records and resolves the owner of an asynchronous task.
 *
 * Recording is best-effort by contract: a lost record only degrades to "task not
 * visible in the status API / no push notification" — it must never break the
 * business operation that started the task.
 */
interface TaskOwnershipRegistryInterface
{
    public function record(string $taskId, string $userIdentifier, ?string $messageType = null): void;

    public function find(string $taskId): ?TaskOwnership;
}
