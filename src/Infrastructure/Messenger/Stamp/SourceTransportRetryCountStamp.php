<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Infrastructure\Messenger\Stamp;

use Symfony\Component\Messenger\Stamp\NonSendableStampInterface;

/**
 * Carries the retry_count of the source outbox/inbox table row while the envelope is
 * being processed, so a reject can persist an incremented count.
 */
final readonly class SourceTransportRetryCountStamp implements NonSendableStampInterface
{
    private int $retryCount;

    public function __construct(string|int $retryCount)
    {
        $this->retryCount = (int) $retryCount;
    }

    public function getRetryCount(): int
    {
        return $this->retryCount;
    }
}
