<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Fixture\Handler;

use Kraz\MessengerWorkflow\Application\Attribute\AsEventHandler;
use Kraz\MessengerWorkflow\Tests\Fixture\Message\TransportScopedEvent;
use Kraz\MessengerWorkflow\Tests\Fixture\MessageRecorder;

#[AsEventHandler(fromTransport: 'app_events')]
final class FromTransportEventHandler
{
    public function __construct(private readonly MessageRecorder $recorder)
    {
    }

    public function __invoke(TransportScopedEvent $event): void
    {
        $this->recorder->record(self::class, $event);
    }
}
