<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Application\Task;

/**
 * Ownership record of an asynchronous task: which user started it, what was
 * dispatched and when. Used to authorize status endpoints and to resolve push
 * recipients from worker processes where no security token exists.
 */
final readonly class TaskOwnership
{
    public function __construct(
        public string $userIdentifier,
        public ?string $messageType,
        public \DateTimeImmutable $startedAt,
    ) {
    }
}
