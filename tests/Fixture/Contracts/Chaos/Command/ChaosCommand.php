<?php

declare(strict_types=1);

namespace Contracts\Chaos\Command;

use Kraz\MessengerWorkflow\Application\CommandInterface;

final class ChaosCommand implements CommandInterface
{
    public function __construct(public readonly string $payload = 'test')
    {
    }
}
