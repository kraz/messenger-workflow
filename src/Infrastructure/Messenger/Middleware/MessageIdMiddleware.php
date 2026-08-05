<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Infrastructure\Messenger\Middleware;

use Kraz\MessengerWorkflow\Infrastructure\Messenger\Stamp\MessageIdStamp;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Middleware\MiddlewareInterface;
use Symfony\Component\Messenger\Middleware\StackInterface;
use Symfony\Component\Uid\Uuid;

/**
 * Guarantees every message carries a stable MessageIdStamp (UUID v7).
 */
class MessageIdMiddleware implements MiddlewareInterface
{
    public function handle(Envelope $envelope, StackInterface $stack): Envelope
    {
        if (null === $envelope->last(MessageIdStamp::class)) {
            $envelope = $envelope->with(new MessageIdStamp((string) Uuid::v7()));
        }

        return $stack->next()->handle($envelope, $stack);
    }
}
