<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Infrastructure\Messenger\Middleware;

use Jwage\PhpAmqpLibMessengerBundle\Transport\AmqpReceivedStamp;
use Kraz\MessengerWorkflow\Application\CommandInterface;
use Kraz\MessengerWorkflow\Application\Task\ResultStorageInterface;
use Kraz\MessengerWorkflow\Infrastructure\Doctrine\Outbox\OutboxTransport;
use Kraz\MessengerWorkflow\Infrastructure\Doctrine\WorkflowTransportRegistry;
use Kraz\MessengerWorkflow\Infrastructure\Messenger\CommandCompletedNotification;
use Kraz\MessengerWorkflow\Infrastructure\Messenger\Stamp\MessageIdStamp;
use Kraz\MessengerWorkflow\Infrastructure\Messenger\Stamp\ResultTrackedStamp;
use Psr\Container\ContainerInterface;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\UnrecoverableMessageHandlingException;
use Symfony\Component\Messenger\Middleware\MiddlewareInterface;
use Symfony\Component\Messenger\Middleware\StackInterface;
use Symfony\Component\Messenger\Stamp\HandledStamp;
use Symfony\Component\Messenger\Stamp\ReceivedStamp;

/**
 * Notifier segment, handler side: when a TRACKED command has been
 * handled, a CommandCompletedNotification carrying the handler result is written to
 * the "<receiving transport>_notifier" outbox. Because this middleware runs inside
 * the WorkflowTransactionMiddleware transaction and the notifier outbox must share
 * the inbox's database connection (validated here), the notification commits
 * atomically with the handler's writes and the inbox row removal.
 *
 * Reduced flows: with no "<receiver>_notifier" transport configured, the result is
 * written to the result storage directly from the handler worker — no outbox
 * guarantee (a dual write), which is the informed trade-off of removing the notifier
 * segment. Untracked commands never touch the notifier or the result storage.
 *
 * The notifier outbox is consumed by a notifier worker running "messenger:consume
 * <name>_notifier --bus=notifier.bus" (the outbox strips bus-name stamps, so the bus
 * must be selected explicitly — worker derivation does this).
 */
class CommandNotifierMiddleware implements MiddlewareInterface
{
    public function __construct(
        private readonly ContainerInterface $receiverLocator,
        private readonly WorkflowTransportRegistry $transportRegistry,
        private readonly ResultStorageInterface $resultStorage,
    ) {
    }

    public function handle(Envelope $envelope, StackInterface $stack): Envelope
    {
        $received = $envelope->last(ReceivedStamp::class);
        if (null === $received
            || null === $envelope->last(ResultTrackedStamp::class)
            || !$envelope->getMessage() instanceof CommandInterface
        ) {
            return $stack->next()->handle($envelope, $stack);
        }

        $messageId = $envelope->last(MessageIdStamp::class)?->getMessageId();
        if (null === $messageId || '' === $messageId) {
            throw new UnrecoverableMessageHandlingException(\sprintf('Tracked command "%s" is missing its message id (Task ID) — the result cannot be published.', get_debug_type($envelope->getMessage())));
        }

        // Resolve and validate the notifier BEFORE running the handler: a
        // misconfigured notifier must fail the message without side effects.
        // Inbox mode: the receiving transport is named after the broker queue, so
        // "<transport>_notifier" is the convention. No-inbox mode: the receiving
        // transport is the broker itself — the queue name (jwage received stamp)
        // keeps the same "<queue>_notifier" convention.
        $receiverName = $received->getTransportName();
        $notifierTransport = $this->resolveNotifierTransport($receiverName);
        if (null === $notifierTransport) {
            $queueName = $envelope->last(AmqpReceivedStamp::class)?->getQueueName();
            if (null !== $queueName && $queueName !== $receiverName) {
                $notifierTransport = $this->resolveNotifierTransport($queueName);
            }
        }

        $envelope = $stack->next()->handle($envelope, $stack);

        $handledStamp = $envelope->last(HandledStamp::class);
        if (null === $handledStamp) {
            // Zero handlers — the exactly-one-handler middleware throws before this point.
            return $envelope;
        }

        if (null !== $notifierTransport) {
            $notifierTransport->send(new Envelope(
                CommandCompletedNotification::forResult($messageId, $handledStamp->getResult()),
            ));
        } else {
            $this->resultStorage->write($messageId, $handledStamp->getResult());
        }

        return $envelope;
    }

    private function resolveNotifierTransport(string $receiverName): ?OutboxTransport
    {
        $notifierName = $receiverName.'_notifier';
        if (!$this->receiverLocator->has($notifierName)) {
            return null;
        }

        $notifierTransport = $this->receiverLocator->get($notifierName);
        if (!$notifierTransport instanceof OutboxTransport) {
            throw new UnrecoverableMessageHandlingException(\sprintf('The notifier transport "%s" must be an outbox transport, got "%s".', $notifierName, get_debug_type($notifierTransport)));
        }

        $inboxTransport = $this->transportRegistry->getInboxTransport($receiverName);
        if (null !== $inboxTransport
            && $this->transportRegistry->isInboxTransactional($receiverName)
            && $notifierTransport->getConnection()->getDriverConnection() !== $inboxTransport->getConnection()->getDriverConnection()
        ) {
            throw new UnrecoverableMessageHandlingException(\sprintf('The notifier outbox "%s" must use the same database connection as inbox transport "%s" so the completion notification is written inside the handler transaction.', $notifierName, $receiverName));
        }

        return $notifierTransport;
    }
}
