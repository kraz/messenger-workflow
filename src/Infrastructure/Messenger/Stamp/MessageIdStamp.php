<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Infrastructure\Messenger\Stamp;

use Symfony\Component\Messenger\Stamp\StampInterface;

/**
 * Stable message identity (UUID v7) — also the Task ID for tracked commands/queries
 * and the deduplication key on inbox transports.
 */
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
