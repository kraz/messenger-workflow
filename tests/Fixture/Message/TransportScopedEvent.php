<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Fixture\Message;

use Kraz\MessengerWorkflow\Domain\DomainEventInterface;

final class TransportScopedEvent implements DomainEventInterface
{
    use EventMetadataTrait;
}
