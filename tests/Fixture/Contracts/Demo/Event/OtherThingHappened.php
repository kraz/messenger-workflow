<?php

declare(strict_types=1);

namespace Contracts\Demo\Event;

use Kraz\MessengerWorkflow\Domain\DomainEventInterface;
use Kraz\MessengerWorkflow\Tests\Fixture\Message\EventMetadataTrait;

/**
 * Contract event with no queue binding — used to prove unbound events are not delivered.
 */
final class OtherThingHappened implements DomainEventInterface
{
    use EventMetadataTrait;
}
