<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Fixture\Handler;

use Kraz\MessengerWorkflow\Application\Attribute\AsEventHandler;
use Kraz\MessengerWorkflow\Tests\Fixture\Message\TestEvent;
use Kraz\MessengerWorkflow\Tests\Fixture\MessageRecorder;

final readonly class TestEventHandlers
{
    public function __construct(
        private MessageRecorder $recorder,
    ) {
    }

    #[AsEventHandler]
    public function onTestEventFirst(TestEvent $event): void
    {
        $this->recorder->record($event);
    }

    #[AsEventHandler]
    public function onTestEventSecond(TestEvent $event): void
    {
        $this->recorder->record($event);
    }
}
