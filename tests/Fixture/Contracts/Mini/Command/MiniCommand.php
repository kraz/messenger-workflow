<?php

declare(strict_types=1);

namespace Contracts\Mini\Command;

use Kraz\MessengerWorkflow\Application\CommandInterface;

final class MiniCommand implements CommandInterface
{
    public function __construct(public readonly string $payload = 'test')
    {
    }
}
