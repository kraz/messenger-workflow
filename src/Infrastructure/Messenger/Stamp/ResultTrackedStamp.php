<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Infrastructure\Messenger\Stamp;

use Symfony\Component\Messenger\Stamp\StampInterface;

/**
 * Marks a message whose outcome must be written to the result storage: the handler
 * result (or failure) is published under the message id as Task ID. Attached by the
 * buses when the caller opted in to result tracking.
 */
final readonly class ResultTrackedStamp implements StampInterface, TransferableStampInterface
{
}
