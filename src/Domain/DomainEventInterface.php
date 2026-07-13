<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Domain;

interface DomainEventInterface
{
    public function withoutMetadata(): self;

    public function withMetadata(mixed ...$fields): self;
}
