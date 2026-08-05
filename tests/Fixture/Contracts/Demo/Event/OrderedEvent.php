<?php

declare(strict_types=1);

namespace Contracts\Demo\Event;

use Kraz\MessengerWorkflow\Domain\DomainEventInterface;
use Kraz\MessengerWorkflow\Tests\Fixture\Message\EventMetadataTrait;

/**
 * Contract event for the ordered-delivery E2E suite; the binding key of the app_events
 * queue is auto-derived from the fixture handler's fromTransport attribute.
 */
final class OrderedEvent implements DomainEventInterface
{
    use EventMetadataTrait;

    public function __construct(public readonly string $payload = 'test')
    {
    }
}
