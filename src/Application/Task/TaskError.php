<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Application\Task;

/**
 * Machine-readable error metadata of a failed task. The exception class is only
 * populated in debug mode; the stored trace is never exposed.
 */
final readonly class TaskError
{
    public function __construct(
        public string $message,
        public int|string|null $code = null,
        public ?string $class = null,
    ) {
    }
}
