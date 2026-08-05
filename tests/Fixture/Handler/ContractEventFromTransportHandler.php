<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Fixture\Handler;

use Contracts\Demo\Event\SomethingHappened;
use Kraz\MessengerWorkflow\Application\Attribute\AsEventHandler;
use Kraz\MessengerWorkflow\Tests\Fixture\MessageRecorder;

#[AsEventHandler(fromTransport: 'app_events')]
final class ContractEventFromTransportHandler
{
    public function __construct(private readonly MessageRecorder $recorder)
    {
    }

    public function __invoke(SomethingHappened $event): void
    {
        $this->recorder->record(self::class, $event);
    }
}
