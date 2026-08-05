<?php

declare(strict_types=1);

namespace Contracts\Demo\Event;

use Kraz\MessengerWorkflow\Domain\DomainEventInterface;
use Kraz\MessengerWorkflow\Tests\Fixture\Message\EventMetadataTrait;

/**
 * Contract event consumed by the gamma context (transactional events inbox).
 */
final class GammaEvent implements DomainEventInterface
{
    use EventMetadataTrait;

    public function __construct(public readonly string $payload = 'test')
    {
    }
}
