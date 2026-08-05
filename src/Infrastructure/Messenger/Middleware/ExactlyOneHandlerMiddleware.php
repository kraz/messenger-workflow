<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Infrastructure\Messenger\Middleware;

use Kraz\MessengerWorkflow\Application\CommandInterface;
use Kraz\MessengerWorkflow\Application\QueryInterface;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\UnrecoverableMessageHandlingException;
use Symfony\Component\Messenger\Middleware\MiddlewareInterface;
use Symfony\Component\Messenger\Middleware\StackInterface;
use Symfony\Component\Messenger\Stamp\HandledStamp;
use Symfony\Component\Messenger\Stamp\ReceivedStamp;

/**
 * Exactly-one-handler enforcement for commands and queries at consume time (D10 —
 * the handler may live in another deployed context, so this cannot be a compile-time
 * check). Zero handlers on a consumed command/query means the consuming context has
 * no handler for it; multiple handlers make the result ambiguous. Both are permanent
 * failures — no retry; when the message is tracked, the failure listener publishes
 * the error to the result storage.
 */
class ExactlyOneHandlerMiddleware implements MiddlewareInterface
{
    public function handle(Envelope $envelope, StackInterface $stack): Envelope
    {
        $envelope = $stack->next()->handle($envelope, $stack);

        if (null === $envelope->last(ReceivedStamp::class)) {
            return $envelope;
        }

        $message = $envelope->getMessage();
        if (!$message instanceof CommandInterface && !$message instanceof QueryInterface) {
            return $envelope;
        }

        $handledStamps = $envelope->all(HandledStamp::class);
        if ([] === $handledStamps) {
            throw new UnrecoverableMessageHandlingException(\sprintf('Message of type "%s" was handled zero times. Exactly one handler is expected.', get_debug_type($message)));
        }

        if (\count($handledStamps) > 1) {
            $handlers = implode(', ', array_map(
                static fn (HandledStamp $stamp): string => \sprintf('"%s"', $stamp->getHandlerName()),
                $handledStamps,
            ));

            throw new UnrecoverableMessageHandlingException(\sprintf('Message of type "%s" was handled multiple times. Exactly one handler is expected, got %d: %s.', get_debug_type($message), \count($handledStamps), $handlers));
        }

        return $envelope;
    }
}
