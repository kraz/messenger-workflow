<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Messenger;

use Kraz\MessengerWorkflow\Application\Messenger\EventBusInterface;
use Kraz\MessengerWorkflow\Domain\DomainEventInterface;
use Kraz\MessengerWorkflow\Messenger\Stamp\EventStamp;
use Kraz\MessengerWorkflow\Messenger\Stamp\MessageIdStamp;
use Kraz\MessengerWorkflow\Messenger\Stamp\StrictOrderStamp;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\MessageBusInterface;
use Symfony\Component\Uid\Uuid;

class EventBus implements EventBusInterface
{
    private MessageBusInterface $messageBus;

    public function __construct(MessageBusInterface $eventBus)
    {
        $this->messageBus = $eventBus;
    }

    public function publish(object $event): void
    {
        $envelope = Envelope::wrap($event);
        $eventStamp = $envelope->last(EventStamp::class);
        if (!$eventStamp) {
            $messageId = $envelope->last(MessageIdStamp::class)?->getMessageId() ?: (string) Uuid::v7();
            $routingKey = (string) RoutingKey::createForTopicTransport($envelope->getMessage(), 'events');
            $eventStamp = new EventStamp(
                messageId: $messageId,
                routingKey: $routingKey,
            );
            $envelope = $envelope->with($eventStamp);
        }

        if (!$envelope->getMessage() instanceof DomainEventInterface) {
            throw new \RuntimeException(\sprintf('Invalid event message. Expected an instance of "%s", but got %s', DomainEventInterface::class, \get_class($envelope->getMessage())));
        }

        if (!$envelope->all(StrictOrderStamp::class)) {
            $envelope = $envelope->with(new StrictOrderStamp());
        }

        $this->messageBus->dispatch($envelope);
    }
}
