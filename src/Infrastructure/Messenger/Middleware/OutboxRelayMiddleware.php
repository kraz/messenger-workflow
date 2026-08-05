<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Infrastructure\Messenger\Middleware;

use Kraz\MessengerWorkflow\Infrastructure\Messenger\AmqpStampFactory;
use Kraz\MessengerWorkflow\Infrastructure\Messenger\Stamp\TransferableStamps;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\LogicException;
use Symfony\Component\Messenger\Middleware\MiddlewareInterface;
use Symfony\Component\Messenger\Middleware\StackInterface;
use Symfony\Component\Messenger\Stamp\HandledStamp;
use Symfony\Component\Messenger\Stamp\ReceivedStamp;
use Symfony\Component\Messenger\Transport\Sender\SendersLocatorInterface;

/**
 * Relays envelopes consumed from an outbox transport to the message broker: builds a
 * fresh envelope from the message plus its transferable stamps (message id, tracking,
 * ordering), attaches the AMQP routing stamp and sends it through the routed sender(s)
 * (the commands/queries/events broker transports).
 *
 * A publish failure bubbles up so the worker rejects the outbox row — which keeps it
 * (retry_count incremented) for a later relay attempt.
 */
class OutboxRelayMiddleware implements MiddlewareInterface
{
    public function __construct(
        private readonly SendersLocatorInterface $sendersLocator,
        private readonly AmqpStampFactory $stampFactory = new AmqpStampFactory(),
    ) {
    }

    public function handle(Envelope $envelope, StackInterface $stack): Envelope
    {
        if (null === $envelope->last(ReceivedStamp::class)) {
            throw new LogicException(\sprintf('The relay bus only consumes messages from outbox transports — "%s" was dispatched directly.', get_debug_type($envelope->getMessage())));
        }

        $outgoing = TransferableStamps::extract($envelope);
        $amqpStamp = $this->stampFactory->createForEnvelope($outgoing);
        if (null !== $amqpStamp) {
            $outgoing = $outgoing->with($amqpStamp);
        }

        $sent = false;
        foreach ($this->sendersLocator->getSenders($outgoing) as $sender) {
            $sender->send($outgoing);
            $sent = true;
        }

        if (!$sent) {
            throw new LogicException(\sprintf('No broker transport is routed for outbox message "%s" — check the messenger routing configuration.', get_debug_type($envelope->getMessage())));
        }

        return $stack->next()->handle(
            $envelope->with(new HandledStamp(null, 'outbox.relay')),
            $stack,
        );
    }
}
