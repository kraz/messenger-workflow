<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Infrastructure\Messenger;

use Kraz\MessengerWorkflow\Application\Messenger\EventBusInterface;
use Kraz\MessengerWorkflow\Domain\DomainEventInterface;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\MessageBusInterface;

/**
 * Event bus: publish() always sends through the broker (or an outbox — pass a
 * TransportNamesStamp-wrapped Envelope, or use an OutboxBus). Message identity and the
 * topic routing key are attached by the bus middlewares; ordering is a property of the
 * consuming transports, not of the published envelope.
 */
class EventBus implements EventBusInterface
{
    public function __construct(
        private readonly MessageBusInterface $messageBus,
    ) {
    }

    public function publish(object $event): void
    {
        $envelope = Envelope::wrap($event);
        if (!$envelope->getMessage() instanceof DomainEventInterface) {
            throw new \RuntimeException(\sprintf('Invalid event message. Expected an instance of "%s", but got %s', DomainEventInterface::class, get_debug_type($envelope->getMessage())));
        }

        $this->messageBus->dispatch($envelope);
    }
}
