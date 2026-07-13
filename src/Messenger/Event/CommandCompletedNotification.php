<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Messenger\Event;

class CommandCompletedNotification
{
    public function __construct(
        protected string $commandId,
        protected mixed $result,
    ) {
    }

    public function getCommandId(): string
    {
        return $this->commandId;
    }

    public function getResult(): mixed
    {
        return $this->result;
    }
}
