<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Messenger\Doctrine\Handler;

use Doctrine\ORM\EntityManagerInterface;
use Kraz\MessengerWorkflow\Application\Messenger\HandlerTransactionInterface;
use Kraz\MessengerWorkflow\Messenger\Doctrine\Connection;
use Kraz\MessengerWorkflow\Messenger\Doctrine\DbalConnectionComparator;
use Kraz\MessengerWorkflow\Messenger\Doctrine\Transport\Outbox\OutboxTransport;
use Symfony\Component\Messenger\Exception\HandlerFailedException;
use Symfony\Component\Messenger\Stamp\HandledStamp;

class DoctrineHandlerTransaction implements HandlerTransactionInterface
{
    public function __construct(
        protected EntityManagerInterface $entityManager,
    ) {
    }

    public function begin(): void
    {
        $this->entityManager->beginTransaction();
    }

    public function commit(): void
    {
        $this->entityManager->commit();
    }

    public function rollback(): void
    {
        $this->entityManager->rollback();
    }

    /**
     * @throws \Throwable
     */
    public function wrap(callable $func): mixed
    {
        $this->entityManager->beginTransaction();
        try {
            $result = $func();
            $this->entityManager->flush();
            $this->entityManager->commit();

            return $result;
        } catch (\Throwable $exception) {
            $this->entityManager->rollback();

            if ($exception instanceof HandlerFailedException) {
                throw new HandlerFailedException($exception->getEnvelope()->withoutAll(HandledStamp::class), $exception->getWrappedExceptions());
            }

            throw $exception;
        }
    }

    public function isSameTransactionProvider(mixed $provider): bool
    {
        if ($provider instanceof EntityManagerInterface) {
            $provider = $provider->getConnection();
        }
        if ($provider instanceof OutboxTransport) {
            $provider = $provider->getConnection();
        }
        if ($provider instanceof Connection) {
            $provider = $provider->getDriverConnection();
        }
        if (!$provider instanceof \Doctrine\DBAL\Connection) {
            return false;
        }

        return DbalConnectionComparator::isSameDatabase($this->entityManager->getConnection(), $provider);
    }
}
