<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Messenger\Stamp;

use Symfony\Component\Messenger\Stamp\StampInterface;

class EventStamp implements StampInterface
{
    public function __construct(
        protected string $messageId,
        protected string $routingKey,
    ) {
    }

    public function getMessageId(): string
    {
        return $this->messageId;
    }

    public function getRoutingKey(): string
    {
        return $this->routingKey;
    }
}
