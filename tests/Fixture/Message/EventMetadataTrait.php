<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Fixture\Message;

trait EventMetadataTrait
{
    /**
     * @var array<array-key, mixed>
     */
    private array $metadata = [];

    public function withoutMetadata(): static
    {
        $clone = clone $this;
        $clone->metadata = [];

        return $clone;
    }

    public function withMetadata(mixed ...$fields): static
    {
        $clone = clone $this;
        $clone->metadata = array_merge($clone->metadata, $fields);

        return $clone;
    }

    /**
     * @return array<array-key, mixed>
     */
    public function getMetadata(): array
    {
        return $this->metadata;
    }
}
