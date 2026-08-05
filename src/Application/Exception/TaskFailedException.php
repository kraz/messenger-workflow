<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Application\Exception;

class TaskFailedException extends \RuntimeException
{
    public function __construct(
        string $message = '',
        int $code = 0,
        private readonly ?string $taskClass = null,
        private readonly ?string $taskTrace = null,
        ?\Throwable $previous = null,
    ) {
        parent::__construct($message, $code, $previous);
    }

    public function getTaskClass(): ?string
    {
        return $this->taskClass;
    }

    public function getTaskTrace(): ?string
    {
        return $this->taskTrace;
    }

    public function __toString(): string
    {
        return parent::__toString().('' !== $this->taskTrace && null !== $this->taskTrace ? \PHP_EOL.'Task trace: '.$this->taskTrace.\PHP_EOL : '');
    }
}
