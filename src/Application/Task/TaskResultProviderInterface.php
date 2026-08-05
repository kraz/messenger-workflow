<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Application\Task;

use Kraz\MessengerWorkflow\Application\Exception\TaskFailedException;
use Kraz\MessengerWorkflow\Application\Task\Exception\TaskNotFoundException;

/**
 * Non-blocking read of a completed task's result value (B8): this is how command
 * task results are read — CommandBusInterface::await() returns void by design.
 */
interface TaskResultProviderInterface
{
    /**
     * Never blocks, never mutates TTLs.
     *
     * @throws TaskNotFoundException when no result exists (unknown, still pending, or expired)
     * @throws TaskFailedException   when the stored outcome is a failure
     */
    public function getResult(string $taskId): mixed;
}
