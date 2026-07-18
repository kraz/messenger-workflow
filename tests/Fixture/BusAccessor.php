<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Fixture;

use Kraz\MessengerWorkflow\Application\Messenger\CommandBusInterface;
use Kraz\MessengerWorkflow\Application\Messenger\EventBusInterface;
use Kraz\MessengerWorkflow\Application\Messenger\QueryBusInterface;

/**
 * Public accessor proving the bus interfaces are autowirable exactly like in application code.
 */
final readonly class BusAccessor
{
    public function __construct(
        public CommandBusInterface $commandBus,
        public QueryBusInterface $queryBus,
        public EventBusInterface $eventBus,
    ) {
    }
}
