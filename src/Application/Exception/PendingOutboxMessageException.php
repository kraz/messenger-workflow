<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Application\Exception;

/**
 * Thrown when awaiting a task whose message is still stored in a transactional outbox
 * on a database connection with an active (uncommitted) transaction in this process.
 *
 * Such a message will never be relayed — and the await would deadlock until timeout —
 * before the surrounding transaction commits.
 */
class PendingOutboxMessageException extends \LogicException
{
}
