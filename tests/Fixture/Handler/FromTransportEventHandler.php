<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Fixture\Handler;

use Kraz\MessengerWorkflow\Application\Attribute\AsEventHandler;
use Kraz\MessengerWorkflow\Tests\Fixture\Message\TransportScopedEvent;
use Kraz\MessengerWorkflow\Tests\Fixture\MessageRecorder;

final readonly class FromTransportEventHandler
{
    public function __construct(
        private MessageRecorder $recorder,
    ) {
    }

    #[AsEventHandler(fromTransport: 'app_events')]
    public function onEvent(TransportScopedEvent $event): void
    {
        $this->recorder->record($event);
    }
}
