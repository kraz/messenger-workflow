<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Application\Exception;

class TaskFailedException extends \RuntimeException
{
    private ?string $taskTrace;
    private ?string $taskClass;

    public function __construct(string $message = '', int $code = 0, ?string $taskClass = null, ?string $taskTrace = null, ?\Throwable $previous = null)
    {
        parent::__construct($message, $code, $previous);
        $this->taskClass = $taskClass;
        $this->taskTrace = $taskTrace;
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
