<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Messenger\Middleware;

use Kraz\MessengerWorkflow\Messenger\Stamp\CommandStamp;
use Kraz\MessengerWorkflow\Messenger\Stamp\EventStamp;
use Kraz\MessengerWorkflow\Messenger\Stamp\MessageIdStamp;
use Kraz\MessengerWorkflow\Messenger\Stamp\QueryStamp;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Middleware\MiddlewareInterface;
use Symfony\Component\Messenger\Middleware\StackInterface;
use Symfony\Component\Uid\Uuid;

class AddMessageIdStampMiddleware implements MiddlewareInterface
{
    public function handle(Envelope $envelope, StackInterface $stack): Envelope
    {
        if (null === $envelope->last(MessageIdStamp::class)) {
            if ($eventStamp = $envelope->last(EventStamp::class)) {
                $envelope = $envelope->with(new MessageIdStamp($eventStamp->getMessageId()));
            } elseif ($queryStamp = $envelope->last(QueryStamp::class)) {
                $envelope = $envelope->with(new MessageIdStamp($queryStamp->getMessageId()));
            } elseif ($commandStamp = $envelope->last(CommandStamp::class)) {
                $envelope = $envelope->with(new MessageIdStamp($commandStamp->getMessageId()));
            } else {
                $envelope = $envelope->with(new MessageIdStamp((string) Uuid::v7()));
            }
        }

        return $stack->next()->handle($envelope, $stack);
    }
}
