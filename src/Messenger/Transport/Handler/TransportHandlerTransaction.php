<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Messenger\Transport\Handler;

use Kraz\MessengerWorkflow\Application\Messenger\HandlerTransactionInterface;
use Kraz\MessengerWorkflow\Messenger\Transport\TransactionalTransportInterface;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\HandlerFailedException;
use Symfony\Component\Messenger\Exception\UnrecoverableMessageHandlingException;
use Symfony\Component\Messenger\Stamp\HandledStamp;
use Symfony\Component\Messenger\Stamp\RedeliveryStamp;

class TransportHandlerTransaction implements HandlerTransactionInterface
{
    public function __construct(
        protected TransactionalTransportInterface $transport,
        protected Envelope $envelope,
    ) {
    }

    public function begin(): void
    {
        $this->transport->beginTransaction();
    }

    public function commit(): void
    {
        $this->transport->commitTransaction();
    }

    public function rollback(): void
    {
        $this->transport->rollbackTransaction();
    }

    public function wrap(callable $func): mixed
    {
        $this->transport->beginTransaction();
        try {
            $result = $func();
            $this->transport->ack($this->envelope);
            $this->transport->commitTransaction();

            return $result;
        } catch (\Throwable $exception) {
            $this->transport->rollbackTransaction();

            if ($exception instanceof HandlerFailedException) {
                $nestedException = $exception->getWrappedExceptions()[0] ?? null;
                if ($nestedException instanceof UnrecoverableMessageHandlingException) {
                    $this->transport->reject($this->envelope
                        ->withoutAll(RedeliveryStamp::class)
                        ->with(new RedeliveryStamp(-1))
                    );
                }
                throw new HandlerFailedException($exception->getEnvelope()->withoutAll(HandledStamp::class), $exception->getWrappedExceptions());
            }

            throw $exception;
        }
    }

    public function isSameTransactionProvider(mixed $provider): bool
    {
        return $this->transport->isSameTransactionProvider($provider);
    }
}
