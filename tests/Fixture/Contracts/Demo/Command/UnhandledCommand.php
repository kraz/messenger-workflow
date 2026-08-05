<?php

declare(strict_types=1);

namespace Contracts\Demo\Command;

use Kraz\MessengerWorkflow\Application\CommandInterface;

/**
 * A contract command no deployed context handles — used to verify the consume-time
 * exactly-one-handler enforcement (D10).
 */
final class UnhandledCommand implements CommandInterface
{
    public function __construct(public readonly string $payload = 'test')
    {
    }
}
