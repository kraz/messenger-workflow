<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Application\Task;

use Kraz\MessengerWorkflow\Application\Task\Exception\TaskNotFoundException;

interface TaskStatusProviderInterface
{
    /**
     * Non-destructive peek at a task's status (never blocks, never mutates TTLs).
     *
     * @throws TaskNotFoundException when neither a result nor an ownership record exists
     */
    public function getStatus(string $taskId): TaskStatusResponse;
}
