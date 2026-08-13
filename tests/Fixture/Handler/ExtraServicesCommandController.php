<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Fixture\Handler;

use Kraz\MessengerWorkflow\Application\Attribute\AsCommandHandler;
use Kraz\MessengerWorkflow\Tests\Fixture\Message\ExtraServicesCommand;
use Kraz\MessengerWorkflow\Tests\Fixture\MessageRecorder;
use Kraz\MessengerWorkflow\Tests\Fixture\NotAService;

/**
 * Controller-style handler: the method declares services after the message instead of
 * constructor injection — the missing nullable service and the scalar default must be
 * resolved too.
 */
final class ExtraServicesCommandController
{
    #[AsCommandHandler]
    public function handle(
        ExtraServicesCommand $command,
        MessageRecorder $recorder,
        ?NotAService $missing = null,
        int $limit = 42,
    ): string {
        $recorder->record(self::class.'::handle', $command);

        return \sprintf('extra-command-result:%s:%s:%d', $command->payload, null === $missing ? 'no-service' : 'service', $limit);
    }
}
