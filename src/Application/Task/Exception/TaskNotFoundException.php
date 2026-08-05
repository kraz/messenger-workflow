<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Application\Task\Exception;

/**
 * Thrown when neither a result nor an ownership record exists for a task id
 * (unknown or already expired).
 */
class TaskNotFoundException extends \RuntimeException
{
}
