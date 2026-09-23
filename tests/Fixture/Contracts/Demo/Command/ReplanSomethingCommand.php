<?php

declare(strict_types=1);

namespace Contracts\Demo\Command;

use Kraz\MessengerWorkflow\Application\Attribute\MessageRoute;
use Kraz\MessengerWorkflow\Application\CommandInterface;

/**
 * Routed to the "planning" queue of the Demo context by its attribute — nothing in
 * the configuration names this class.
 */
#[MessageRoute('planning')]
final class ReplanSomethingCommand implements CommandInterface
{
    public function __construct(public readonly string $payload = 'replan')
    {
    }
}
