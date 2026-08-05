<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Infrastructure\Messenger\Middleware;

use Jwage\PhpAmqpLibMessengerBundle\Transport\AmqpReceivedStamp;
use Kraz\MessengerWorkflow\Infrastructure\Messenger\Stamp\TransferableStamps;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\LogicException;
use Symfony\Component\Messenger\Middleware\MiddlewareInterface;
use Symfony\Component\Messenger\Middleware\StackInterface;
use Symfony\Component\Messenger\Stamp\HandledStamp;
use Symfony\Component\Messenger\Stamp\ReceivedStamp;
use Symfony\Component\Messenger\Stamp\TransportNamesStamp;
use Symfony\Component\Messenger\Transport\Sender\SendersLocatorInterface;

/**
 * Receiver segment (broker → inbox): moves a message consumed from an AMQP queue into
 * the inbox transport of the same name (convention: broker queue name == inbox
 * transport name), preserving the transferable stamps. Duplicate deliveries are
 * dropped by the inbox dedup index. A storage failure bubbles up so the worker nacks
 * the broker message for redelivery.
 */
class InboxRelayMiddleware implements MiddlewareInterface
{
    public function __construct(
        private readonly SendersLocatorInterface $sendersLocator,
    ) {
    }

    public function handle(Envelope $envelope, StackInterface $stack): Envelope
    {
        if (null === $envelope->last(ReceivedStamp::class)) {
            throw new LogicException(\sprintf('The inbox bus only consumes messages from broker transports — "%s" was dispatched directly.', get_debug_type($envelope->getMessage())));
        }

        $queueName = $envelope->last(AmqpReceivedStamp::class)?->getQueueName();
        if (null === $queueName || '' === $queueName) {
            throw new LogicException(\sprintf('The inbox bus expects messages received from an AMQP queue, got "%s" without an AMQP received stamp.', get_debug_type($envelope->getMessage())));
        }

        $outgoing = TransferableStamps::extract($envelope)
            ->with(new TransportNamesStamp([$queueName]));

        $sent = false;
        foreach ($this->sendersLocator->getSenders($outgoing) as $sender) {
            $sender->send($outgoing);
            $sent = true;
        }

        if (!$sent) {
            throw new LogicException(\sprintf('No inbox transport named "%s" is configured for the consumed broker queue.', $queueName));
        }

        return $stack->next()->handle(
            $envelope->with(new HandledStamp(null, 'inbox.relay')),
            $stack,
        );
    }
}
