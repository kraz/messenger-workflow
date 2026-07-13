<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Messenger\Middleware;

use Kraz\MessengerWorkflow\Messenger\Event\CommandCompletedNotification;
use Kraz\MessengerWorkflow\Messenger\Transport\ResultStorageInterface;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Middleware\MiddlewareInterface;
use Symfony\Component\Messenger\Middleware\StackInterface;
use Symfony\Component\Messenger\Stamp\HandledStamp;
use Symfony\Component\Messenger\Stamp\ReceivedStamp;
use Webmozart\Assert\Assert;

class OutboxMiddleware implements MiddlewareInterface
{
    public function __construct(
        protected ResultStorageInterface $resultStorage,
    ) {
    }

    public function handle(Envelope $envelope, StackInterface $stack): Envelope
    {
        if (!$envelope->all(ReceivedStamp::class)) {
            return $stack->next()->handle($envelope, $stack)
                ->with(new HandledStamp(null, 'outbox'));
        }
        $message = $envelope->getMessage();
        if ($message instanceof CommandCompletedNotification) {
            $this->notifyForCompletedCommand($message);

            return $envelope->with(new HandledStamp(null, 'outbox'));
        }
        throw new \RuntimeException('Unexpected message workflow. The outbox bus is not supposed to receive or handle messages!');
    }

    private function notifyForCompletedCommand(CommandCompletedNotification $notification): void
    {
        $commandId = $notification->getCommandId();
        Assert::stringNotEmpty($commandId);
        $result = $notification->getResult();
        $this->resultStorage->write($commandId, $result);
    }
}
