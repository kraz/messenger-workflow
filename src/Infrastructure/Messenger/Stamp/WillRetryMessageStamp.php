<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Infrastructure\Messenger\Stamp;

use Symfony\Component\Messenger\Stamp\NonSendableStampInterface;

/**
 * Added by the failed-message listener when a failed message will be retried, so the
 * transport reject() keeps the message row instead of removing it permanently.
 */
final readonly class WillRetryMessageStamp implements NonSendableStampInterface
{
}
