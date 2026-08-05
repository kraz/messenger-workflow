<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Infrastructure\Messenger\Middleware;

use Kraz\MessengerWorkflow\Application\Task\ResultStorageInterface;
use Kraz\MessengerWorkflow\Infrastructure\Messenger\CommandCompletedNotification;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\LogicException;
use Symfony\Component\Messenger\Exception\UnrecoverableMessageHandlingException;
use Symfony\Component\Messenger\Middleware\MiddlewareInterface;
use Symfony\Component\Messenger\Middleware\StackInterface;
use Symfony\Component\Messenger\Stamp\HandledStamp;
use Symfony\Component\Messenger\Stamp\ReceivedStamp;

/**
 * Notifier segment, worker side: consumes CommandCompletedNotification messages from
 * a notifier outbox and publishes the captured command result to the result storage.
 * A storage failure bubbles up so the worker rejects the outbox row — which keeps it
 * (retry_count incremented) for a later attempt.
 */
class ResultNotifierMiddleware implements MiddlewareInterface
{
    public function __construct(
        private readonly ResultStorageInterface $resultStorage,
    ) {
    }

    public function handle(Envelope $envelope, StackInterface $stack): Envelope
    {
        if (null === $envelope->last(ReceivedStamp::class)) {
            throw new LogicException(\sprintf('The notifier bus only consumes messages from notifier outbox transports — "%s" was dispatched directly.', get_debug_type($envelope->getMessage())));
        }

        $message = $envelope->getMessage();
        if (!$message instanceof CommandCompletedNotification) {
            throw new UnrecoverableMessageHandlingException(\sprintf('The notifier bus expects "%s" messages, got "%s".', CommandCompletedNotification::class, get_debug_type($message)));
        }

        $this->resultStorage->write($message->getCommandId(), $message->restoreResult());

        return $stack->next()->handle(
            $envelope->with(new HandledStamp(null, 'notifier')),
            $stack,
        );
    }
}
