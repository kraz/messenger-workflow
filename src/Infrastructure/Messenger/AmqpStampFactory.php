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
 * commands/queries route on direct exchanges by bounded-context segment, events on the
 * topic exchange by full dotted path; the MessageIdStamp id becomes the native AMQP
 * message_id property.
 */
final class AmqpStampFactory
{
    public function createForEnvelope(Envelope $envelope): ?AmqpStamp
    {
        $message = $envelope->getMessage();
        $routingKey = match (true) {
            $message instanceof DomainEventInterface => RoutingKey::createForTopicTransport($message, 'events'),
            $message instanceof CommandInterface => RoutingKey::createForDirectTransport($message, 'commands'),
            $message instanceof QueryInterface => RoutingKey::createForDirectTransport($message, 'queries'),
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
}
