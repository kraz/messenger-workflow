<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Fixture\Message;

use Kraz\MessengerWorkflow\Application\CommandInterface;

final class ExtraServicesCommand implements CommandInterface
{
    public function __construct(public readonly string $payload = 'extra')
    {
    }
}
