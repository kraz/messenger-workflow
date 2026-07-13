<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Messenger\Stamp;

use Symfony\Component\Messenger\Stamp\NonSendableStampInterface;

class SourceTransportRetryCountStamp implements NonSendableStampInterface
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
