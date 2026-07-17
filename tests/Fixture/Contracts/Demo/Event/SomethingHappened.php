<?php

declare(strict_types=1);

namespace Contracts\Demo\Event;

use Kraz\MessengerWorkflow\Domain\DomainEventInterface;
use Kraz\MessengerWorkflow\Tests\Fixture\Message\EventMetadataTrait;

final class SomethingHappened implements DomainEventInterface
{
    use EventMetadataTrait;

    public function __construct(
        public string $what = 'it',
    ) {
    }
}
