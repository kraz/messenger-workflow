<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Messenger\Transport;

class ResultStoragePayload
{
    public function __construct(
        protected mixed $value,
        protected ?array $error = null,
    ) {
    }

    public function getValue(): mixed
    {
        return $this->value;
    }

    public function getError(): ?array
    {
        return $this->error;
    }

    public function isError(): bool
    {
        return \is_array($this->error);
    }
}
