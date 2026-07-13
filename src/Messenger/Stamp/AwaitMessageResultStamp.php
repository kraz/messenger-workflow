<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Messenger\Stamp;

use Symfony\Component\Messenger\Stamp\NonSendableStampInterface;

class AwaitMessageResultStamp implements NonSendableStampInterface
{
    public function __construct(
        protected int $timeout,
        protected bool $deferred = false,
    ) {
    }

    public function getTimeout(): int
    {
        return $this->timeout;
    }

    public function isDeferred(): bool
    {
        return $this->deferred;
    }
}
