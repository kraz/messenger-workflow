<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Infrastructure\Messenger\Middleware;

use Jwage\PhpAmqpLibMessengerBundle\Transport\AmqpStamp;
use Kraz\MessengerWorkflow\Infrastructure\Messenger\AmqpStampFactory;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Middleware\MiddlewareInterface;
use Symfony\Component\Messenger\Middleware\StackInterface;
use Symfony\Component\Messenger\Stamp\ReceivedStamp;

/**
 * Attaches the AMQP routing key (and message-id attribute) for outgoing workflow
 * messages via the jwage AmqpStamp. Runs after MessageIdMiddleware (which guarantees
 * the MessageIdStamp).
 */
class AmqpRoutingMiddleware implements MiddlewareInterface
{
    public function __construct(
        private readonly AmqpStampFactory $stampFactory = new AmqpStampFactory(),
    ) {
    }

    public function handle(Envelope $envelope, StackInterface $stack): Envelope
    {
        if (null === $envelope->last(ReceivedStamp::class) && null === $envelope->last(AmqpStamp::class)) {
            $stamp = $this->stampFactory->createForEnvelope($envelope);

            if (null !== $stamp) {
                $envelope = $envelope->with($stamp);
            }
        }

        return $stack->next()->handle($envelope, $stack);
    }
}
