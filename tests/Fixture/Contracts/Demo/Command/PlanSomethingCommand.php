<?php

declare(strict_types=1);

namespace Contracts\Demo\Command;

use Kraz\MessengerWorkflow\Application\CommandInterface;

/**
 * Routed to the "planning" queue of the Demo context through the queue binding's
 * "messages" list (RoutedCommandsKernel).
 */
final class PlanSomethingCommand implements CommandInterface
{
    public function __construct(public readonly string $payload = 'plan')
    {
    }
}
