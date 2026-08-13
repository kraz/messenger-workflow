<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Fixture\Message;

use Kraz\MessengerWorkflow\Domain\DomainEventInterface;

final class ExtraServicesEvent implements DomainEventInterface
{
    use EventMetadataTrait;

    public function __construct(public readonly string $payload = 'extra')
    {
    }
}
