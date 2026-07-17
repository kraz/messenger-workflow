<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Messenger\Middleware;

use Kraz\MessengerWorkflow\Messenger\Stamp\StrictOrderStamp;
use Kraz\MessengerWorkflow\Messenger\Stamp\TargetTransportNameStamp;
use Kraz\MessengerWorkflow\Messenger\Stamp\TransferableStamps;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\RecoverableMessageHandlingException;
use Symfony\Component\Messenger\Exception\UnrecoverableMessageHandlingException;
use Symfony\Component\Messenger\MessageBusInterface;
use Symfony\Component\Messenger\Middleware\MiddlewareInterface;
use Symfony\Component\Messenger\Middleware\StackInterface;
use Symfony\Component\Messenger\Stamp\HandledStamp;
use Symfony\Component\Messenger\Stamp\ReceivedStamp;
use Symfony\Component\Messenger\Stamp\TransportNamesStamp;

class InboxMiddleware implements MiddlewareInterface
{
    public function __construct(
        protected MessageBusInterface $inboxBus,
    ) {
    }

    public function handle(Envelope $envelope, StackInterface $stack): Envelope
    {
        if ($envelope->all(ReceivedStamp::class)) {
            $transportName = $envelope->last(TargetTransportNameStamp::class)?->getTransportName();
            if (!$transportName) {
                $msg = $envelope->getMessage();
                throw new \RuntimeException(\sprintf('Can not publish the message "%s" to the inbox. The target transport name can not be determined!', $msg::class));
            }
            try {
                $inboxEnvelope = TransferableStamps::extract($envelope);
                $inboxEnvelope = $inboxEnvelope
                    ->with(new TransportNamesStamp([$transportName]));
                $this->inboxBus->dispatch($inboxEnvelope);
            } catch (\Exception $exception) {
                if ($envelope->all(StrictOrderStamp::class)) {
                    throw new UnrecoverableMessageHandlingException($exception->getMessage(), $exception->getCode(), $exception);
                }
                throw new RecoverableMessageHandlingException($exception->getMessage(), $exception->getCode(), $exception);
            }

            return $envelope->with(new HandledStamp(null, 'inbox'));
        }

        return $stack->next()->handle($envelope, $stack);
    }
}
