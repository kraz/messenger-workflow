<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Infrastructure\Messenger;

use Jwage\PhpAmqpLibMessengerBundle\Transport\AmqpStamp;
use Kraz\MessengerWorkflow\Application\CommandInterface;
use Kraz\MessengerWorkflow\Application\QueryInterface;
use Kraz\MessengerWorkflow\Domain\DomainEventInterface;
use Kraz\MessengerWorkflow\Infrastructure\Messenger\Stamp\MessageIdStamp;
use Symfony\Component\Messenger\Envelope;

/**
 * Builds the jwage AmqpStamp (routing key + AMQP attributes) for a workflow message:
 * commands/queries route on direct exchanges by bounded-context segment — plus the
 * message's route segment when it belongs to a dedicated queue —, events on the topic
 * exchange by full dotted path; the MessageIdStamp id becomes the native AMQP
 * message_id property.
 *
 * The routing key is a pure function of the message class: the dispatching bus and the
 * outbox relay both derive it from this one factory, so a message sitting in an outbox
 * across a deploy is routed by the configuration current at relay time.
 */
final class AmqpStampFactory
{
    public function __construct(
        private readonly MessageRouteResolver $routes = new MessageRouteResolver(),
    ) {
    }

    public function createForEnvelope(Envelope $envelope): ?AmqpStamp
    {
        $message = $envelope->getMessage();
        $routingKey = match (true) {
            $message instanceof DomainEventInterface => RoutingKey::createForTopicTransport($message, 'events'),
            $message instanceof CommandInterface => $this->createDirectRoutingKey($message, 'commands'),
            $message instanceof QueryInterface => $this->createDirectRoutingKey($message, 'queries'),
            default => null,
        };

        if (null === $routingKey) {
            return null;
        }

        $attributes = [];
        $messageIdStamp = $envelope->last(MessageIdStamp::class);
        if (null !== $messageIdStamp) {
            $attributes['message_id'] = $messageIdStamp->getMessageId();
        }

        return new AmqpStamp((string) $routingKey, $attributes);
    }

    private function createDirectRoutingKey(object $message, string $broker): RoutingKey
    {
        $routingKey = RoutingKey::createForDirectTransport($message, $broker);
        $route = $this->routes->resolve($message);

        return null === $route ? $routingKey : $routingKey->withRoute($route);
    }
}
