<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Messenger\Stamp;

use Symfony\Component\Messenger\Stamp\StampInterface;

class QueryStamp implements StampInterface
{
    public function __construct(
        protected string $messageId,
        protected string $routingKey,
        protected ?int $priority = null,
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

    public function getPriority(): ?int
    {
        return $this->priority;
    }
}
