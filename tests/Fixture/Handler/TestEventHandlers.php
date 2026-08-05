<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Fixture\Handler;

use Kraz\MessengerWorkflow\Application\Attribute\AsEventHandler;
use Kraz\MessengerWorkflow\Tests\Fixture\Message\TestEvent;
use Kraz\MessengerWorkflow\Tests\Fixture\MessageRecorder;

/**
 * Two method-level event handlers for the same event — events support 0..n handlers.
 */
final class TestEventHandlers
{
    public function __construct(private readonly MessageRecorder $recorder)
    {
    }

    #[AsEventHandler]
    public function first(TestEvent $event): void
    {
        $this->recorder->record(self::class.'::first', $event);
    }

    #[AsEventHandler]
    public function second(TestEvent $event): void
    {
        $this->recorder->record(self::class.'::second', $event);
    }
}
