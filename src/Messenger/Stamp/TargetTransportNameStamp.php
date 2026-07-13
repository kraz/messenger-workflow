<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Messenger\Stamp;

use Symfony\Component\Messenger\Stamp\StampInterface;

class TargetTransportNameStamp implements StampInterface, TransferableStampInterface
{
    public function __construct(
        protected string $transportName,
    ) {
    }

    public function getTransportName(): string
    {
        return $this->transportName;
    }
}
