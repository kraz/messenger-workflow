<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Messenger\Doctrine\Handler;

use Doctrine\ORM\EntityManagerInterface;
use Kraz\MessengerWorkflow\Application\Messenger\HandlerTransactionInterface;
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
        if (!$provider instanceof \Doctrine\DBAL\Connection) {
            return false;
        }
        if ($provider === $this->entityManager->getConnection()) {
            return true;
        }
        $paramsA = $this->entityManager->getConnection()->getParams();
        $paramsB = $provider->getParams();
        $base = array_intersect_key($paramsA, $paramsB);
        $paramsA = array_intersect_key($paramsA, $base);
        $paramsB = array_intersect_key($paramsB, $base);

        return 0 === \count(array_diff_uassoc($paramsA, $paramsB, strcasecmp(...)));
    }
}
