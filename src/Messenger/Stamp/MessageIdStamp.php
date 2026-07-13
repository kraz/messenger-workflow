<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Messenger\Stamp;

use Symfony\Component\Messenger\Stamp\StampInterface;

final readonly class MessageIdStamp implements StampInterface, TransferableStampInterface
{
    public function __construct(
        private string $messageId,
    ) {
    }

    public function getMessageId(): string
    {
        return $this->messageId;
    }
}
