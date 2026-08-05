<?php

declare(strict_types=1);

namespace Contracts\Nano\Command;

use Kraz\MessengerWorkflow\Application\CommandInterface;

final class NanoCommand implements CommandInterface
{
    public function __construct(public readonly string $payload = 'test')
    {
    }
}
