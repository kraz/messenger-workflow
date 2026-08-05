<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Fixture\Handler;

use Kraz\MessengerWorkflow\Application\Attribute\AsCommandHandler;
use Kraz\MessengerWorkflow\Tests\Fixture\Message\TestCommand;
use Kraz\MessengerWorkflow\Tests\Fixture\MessageRecorder;

#[AsCommandHandler]
final class TestCommandHandler
{
    public function __construct(private readonly MessageRecorder $recorder)
    {
    }

    public function __invoke(TestCommand $command): string
    {
        $this->recorder->record(self::class, $command);

        return 'command-result:'.$command->payload;
    }
}
