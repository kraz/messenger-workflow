<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Infrastructure\Messenger\Middleware;

use Kraz\MessengerWorkflow\Application\QueryInterface;
use Kraz\MessengerWorkflow\Application\Task\ResultStorageInterface;
use Kraz\MessengerWorkflow\Infrastructure\Messenger\Stamp\MessageIdStamp;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\UnrecoverableMessageHandlingException;
use Symfony\Component\Messenger\Middleware\MiddlewareInterface;
use Symfony\Component\Messenger\Middleware\StackInterface;
use Symfony\Component\Messenger\Stamp\HandledStamp;
use Symfony\Component\Messenger\Stamp\ReceivedStamp;

/**
 * Query flow, handler side: the handler worker writes the query result to the
 * result storage directly — queries have no notifier segment; the result storage IS
 * their result channel. A storage failure bubbles up (transient → retried per the
 * query retry policy).
 */
class QueryResultMiddleware implements MiddlewareInterface
{
    public function __construct(
        private readonly ResultStorageInterface $resultStorage,
    ) {
    }

    public function handle(Envelope $envelope, StackInterface $stack): Envelope
    {
        if (null === $envelope->last(ReceivedStamp::class) || !$envelope->getMessage() instanceof QueryInterface) {
            return $stack->next()->handle($envelope, $stack);
        }

        $messageId = $envelope->last(MessageIdStamp::class)?->getMessageId();
        if (null === $messageId || '' === $messageId) {
            throw new UnrecoverableMessageHandlingException(\sprintf('Query "%s" is missing its message id (Task ID) — the result cannot be published.', get_debug_type($envelope->getMessage())));
        }

        $envelope = $stack->next()->handle($envelope, $stack);

        $handledStamp = $envelope->last(HandledStamp::class);
        if (null !== $handledStamp) {
            $this->resultStorage->write($messageId, $handledStamp->getResult());
        }

        return $envelope;
    }
}
