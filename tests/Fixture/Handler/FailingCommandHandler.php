<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Fixture\Handler;

use Kraz\MessengerWorkflow\Application\Attribute\AsCommandHandler;
use Kraz\MessengerWorkflow\Tests\Fixture\Message\FailingCommand;

#[AsCommandHandler]
final readonly class FailingCommandHandler
{
    public function __invoke(FailingCommand $command): void
    {
        throw new \DomainException($command->reason, 42);
    }
}
