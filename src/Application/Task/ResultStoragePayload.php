<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Application\Task;

use Kraz\MessengerWorkflow\Application\Exception\TaskFailedException;

class ResultStoragePayload
{
    /**
     * @param array<string, mixed>|null $error
     */
    public function __construct(
        protected mixed $value,
        protected ?array $error = null,
    ) {
    }

    public function getValue(): mixed
    {
        return $this->value;
    }

    /**
     * @return array<string, mixed>|null
     */
    public function getError(): ?array
    {
        return $this->error;
    }

    public function isError(): bool
    {
        return \is_array($this->error);
    }

    /**
     * Translates an error payload into the exception thrown to the awaiting caller.
     */
    public function createFailure(): TaskFailedException
    {
        $error = $this->error ?? [];

        $message = \is_string($error['message'] ?? null) ? $error['message'] : '';
        $code = $error['code'] ?? 0;
        $code = \is_int($code) ? $code : (\is_string($code) && is_numeric($code) ? (int) $code : 0);
        $class = \is_string($error['class'] ?? null) ? $error['class'] : null;
        $trace = \is_string($error['trace'] ?? null) ? $error['trace'] : null;

        return new TaskFailedException($message, $code, $class, $trace);
    }
}
