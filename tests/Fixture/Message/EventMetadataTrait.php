<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Fixture\Message;

use Kraz\MessengerWorkflow\Domain\DomainEventInterface;

trait EventMetadataTrait
{
    public array $metadata = [];

    public function withoutMetadata(): DomainEventInterface
    {
        $clone = clone $this;
        $clone->metadata = [];

        return $clone;
    }

    public function withMetadata(mixed ...$fields): DomainEventInterface
    {
        $clone = clone $this;
        $clone->metadata = array_merge($clone->metadata, $fields);

        return $clone;
    }
}
