<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Application\Task;

/**
 * Read-only status of an asynchronous task. Deliberately never carries the task
 * result value — read that through TaskResultProviderInterface (or the owning
 * context's regular read endpoints).
 */
final readonly class TaskStatusResponse
{
    public function __construct(
        public string $taskId,
        public TaskStatus $status,
        public ?TaskError $error = null,
        public ?\DateTimeImmutable $startedAt = null,
    ) {
    }
}
