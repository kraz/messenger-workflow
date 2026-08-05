<?php

declare(strict_types=1);

namespace Contracts\Mini\Event;

use Kraz\MessengerWorkflow\Domain\DomainEventInterface;
use Kraz\MessengerWorkflow\Tests\Fixture\Message\EventMetadataTrait;

final class MiniEvent implements DomainEventInterface
{
    use EventMetadataTrait;

    public function __construct(public readonly string $payload = 'test')
    {
    }
}
