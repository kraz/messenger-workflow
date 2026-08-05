<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Infrastructure\Messenger\Middleware;

use Doctrine\DBAL\Connection as DbalConnection;
use Doctrine\ORM\EntityManagerInterface;
use Doctrine\Persistence\ManagerRegistry;
use Jwage\PhpAmqpLibMessengerBundle\Transport\AmqpReceivedStamp;
use Kraz\MessengerWorkflow\Infrastructure\Doctrine\WorkflowTransportRegistry;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\UnrecoverableMessageHandlingException;
use Symfony\Component\Messenger\Middleware\MiddlewareInterface;
use Symfony\Component\Messenger\Middleware\StackInterface;
use Symfony\Component\Messenger\Stamp\ReceivedStamp;

/**
 * The inbox-pattern guarantee: when a message is received from a transactional
 * inbox transport, a database transaction is opened on that transport's connection,
 * the handlers run inside it, and on success the inbox row is removed (ack) INSIDE the
 * same transaction before committing. Application writes on the same connection join
 * the transaction automatically. On failure everything rolls back — the row stays and
 * the normal retry/failure flow proceeds (the worker's later ack is an idempotent no-op
 * for the already-removed row).
 *
 * No-inbox mode: when the handler worker consumes a broker queue directly and
 * the queue is mapped via `messenger_workflow.messenger.transports.<t>.orm_mappings`,
 * a plain transaction on the mapped entity manager's (or DBAL connection's) database
 * wraps the handlers — commit on success, rollback on failure. There is no dedup and
 * the broker ack happens outside the transaction (at-least-once): removing the inbox
 * is the user's informed trade-off, handlers must be idempotent.
 */
class WorkflowTransactionMiddleware implements MiddlewareInterface
{
    /**
     * @param array<string, array<string, string>> $queueOrmBinding broker transport name → queue name → entity manager (or DBAL connection) name
     */
    public function __construct(
        private readonly WorkflowTransportRegistry $transportRegistry,
        private readonly array $queueOrmBinding = [],
        private readonly ?ManagerRegistry $doctrine = null,
    ) {
    }

    public function handle(Envelope $envelope, StackInterface $stack): Envelope
    {
        $transportName = $envelope->last(ReceivedStamp::class)?->getTransportName();
        $transport = null !== $transportName && $this->transportRegistry->isInboxTransactional($transportName)
            ? $this->transportRegistry->getInboxTransport($transportName)
            : null;

        if (null === $transport) {
            $mappedConnection = null !== $transportName ? $this->resolveMappedConnection($envelope, $transportName) : null;
            if (null === $mappedConnection) {
                return $stack->next()->handle($envelope, $stack);
            }

            $mappedConnection->beginTransaction();
            try {
                $result = $stack->next()->handle($envelope, $stack);

                $mappedConnection->commit();

                return $result;
            } catch (\Throwable $exception) {
                if ($mappedConnection->isTransactionActive()) {
                    $mappedConnection->rollBack();
                }

                throw $exception;
            }
        }

        $driverConnection = $transport->getConnection()->getDriverConnection();

        $driverConnection->beginTransaction();
        try {
            $result = $stack->next()->handle($envelope, $stack);

            // Remove the inbox row (and mark the dedup index entry processed) inside
            // the very same transaction as the handler's application writes.
            $transport->ack($result);

            $driverConnection->commit();

            return $result;
        } catch (\Throwable $exception) {
            if ($driverConnection->isTransactionActive()) {
                $driverConnection->rollBack();
            }

            throw $exception;
        }
    }

    private function resolveMappedConnection(Envelope $envelope, string $transportName): ?DbalConnection
    {
        if ([] === $this->queueOrmBinding || null === $this->doctrine) {
            return null;
        }

        $queueName = $envelope->last(AmqpReceivedStamp::class)?->getQueueName();
        $mapped = null !== $queueName ? ($this->queueOrmBinding[$transportName][$queueName] ?? null) : null;
        if (null === $mapped || '' === $mapped) {
            return null;
        }

        // The mapping value is an entity manager name; a plain DBAL connection name is
        // accepted as a fallback for contexts without an ORM entity manager.
        if (\array_key_exists($mapped, $this->doctrine->getManagerNames())) {
            $manager = $this->doctrine->getManager($mapped);
            if ($manager instanceof EntityManagerInterface) {
                return $manager->getConnection();
            }
        }
        if (\array_key_exists($mapped, $this->doctrine->getConnectionNames())) {
            $connection = $this->doctrine->getConnection($mapped);
            if ($connection instanceof DbalConnection) {
                return $connection;
            }
        }

        throw new UnrecoverableMessageHandlingException(\sprintf('orm_mappings maps queue "%s" to "%s", which is neither a Doctrine entity manager nor a DBAL connection name.', $queueName ?? '', $mapped));
    }
}
