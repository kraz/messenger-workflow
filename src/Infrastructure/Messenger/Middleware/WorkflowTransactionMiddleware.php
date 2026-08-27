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
 *
 * ## Closing the unit of work
 *
 * Opening the transaction is only half of a transaction boundary; the other half is
 * telling Doctrine to write. Before returning, every **ORM entity manager running on
 * the transaction's connection** is flushed — inside the transaction, before the
 * inbox row is removed. A handler therefore does not have to flush, and an ORM
 * application gets a real unit of work per message: load aggregates, change them, and
 * the boundary writes what changed. Anything a handler flushed itself simply leaves
 * nothing for this flush to do.
 *
 * The managers to flush are resolved **by connection name at container compile time**
 * ({@see \Kraz\MessengerWorkflow\Infrastructure\DependencyInjection\Compiler\ResolveTransactionEntityManagersPass}):
 * the inbox transport and the entity managers both name their connection in
 * configuration and resolve the same `doctrine.dbal.<name>_connection` service, so the
 * compiled `connection name → entity manager names` map is exactly the set of managers
 * taking part in *this* message's transaction. Per message that is one hash lookup —
 * the other bounded contexts' managers are never instantiated, never scanned, and (as
 * with managers running their own transactions, such as projection writers) left
 * alone. In debug mode, a manager OUTSIDE that set left holding scheduled changes at
 * the boundary fails the message loudly instead of losing the writes silently.
 *
 * Turn it off with `messenger_workflow.messenger.transaction.flush_entity_managers: false`
 * when the application flushes explicitly and wants no implicit write at the boundary.
 *
 * **Only received messages are wrapped**, which is also what keeps the flush safe:
 * these buses are used to *dispatch* as well, and the outbox publishes through the event
 * bus from inside a flush. A dispatch carries no {@see ReceivedStamp}, so it returns
 * above without ever reaching `flush()` and cannot re-enter one.
 */
class WorkflowTransactionMiddleware implements MiddlewareInterface
{
    /**
     * @param array<string, array<string, string>> $queueOrmBinding          broker transport name → queue name → entity manager (or DBAL connection) name
     * @param bool                                 $flushEntityManagers      whether to flush the transaction's entity managers before committing
     * @param array<string, list<string>>          $connectionEntityManagers DBAL connection name → names of the entity managers on it, compiled by ResolveTransactionEntityManagersPass
     * @param bool                                 $debug                    fail the message when a handler leaves scheduled changes in an entity manager outside the transaction (kernel.debug)
     */
    public function __construct(
        private readonly WorkflowTransportRegistry $transportRegistry,
        private readonly array $queueOrmBinding = [],
        private readonly ?ManagerRegistry $doctrine = null,
        private readonly bool $flushEntityManagers = true,
        private readonly array $connectionEntityManagers = [],
        private readonly bool $debug = false,
    ) {
    }

    public function handle(Envelope $envelope, StackInterface $stack): Envelope
    {
        $transportName = $envelope->last(ReceivedStamp::class)?->getTransportName();
        $transport = null !== $transportName && $this->transportRegistry->isInboxTransactional($transportName)
            ? $this->transportRegistry->getInboxTransport($transportName)
            : null;

        if (null === $transport) {
            $mapped = null !== $transportName ? $this->resolveMappedTransaction($envelope, $transportName) : null;
            if (null === $mapped) {
                return $stack->next()->handle($envelope, $stack);
            }
            [$mappedConnection, $managerNames] = $mapped;

            $mappedConnection->beginTransaction();
            try {
                $result = $stack->next()->handle($envelope, $stack);

                $this->flushManagers($managerNames);
                $this->assertNoPendingChangesOutsideTransaction($managerNames, $transportName);

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
        $connectionName = $this->transportRegistry->getInboxConnectionName($transportName)
            ?? $this->resolveConnectionName($driverConnection);
        $managerNames = null !== $connectionName ? ($this->connectionEntityManagers[$connectionName] ?? []) : [];

        $driverConnection->beginTransaction();
        try {
            $result = $stack->next()->handle($envelope, $stack);

            // Write the handler's pending ORM changes first: a failure here must leave
            // the inbox row in place, so the message is retried rather than lost.
            $this->flushManagers($managerNames);
            $this->assertNoPendingChangesOutsideTransaction($managerNames, $transportName);

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

    /**
     * @param list<string> $managerNames
     */
    private function flushManagers(array $managerNames): void
    {
        if (!$this->flushEntityManagers || null === $this->doctrine) {
            return;
        }

        foreach ($managerNames as $managerName) {
            $manager = $this->doctrine->getManager($managerName);
            // A manager closed by an earlier failure cannot flush; leaving it to
            // Doctrine keeps the original error as the reported one.
            if ($manager instanceof EntityManagerInterface && $manager->isOpen()) {
                $manager->flush();
            }
        }
    }

    /**
     * Fallback for inbox transports registered without their connection name
     * (programmatic setups): the name whose registry connection is this instance.
     */
    private function resolveConnectionName(DbalConnection $connection): ?string
    {
        if (null === $this->doctrine) {
            return null;
        }

        foreach ($this->doctrine->getConnections() as $name => $candidate) {
            if ($candidate === $connection) {
                return $name;
            }
        }

        return null;
    }

    /**
     * Debug-only tripwire for the silent-loss edge the compiled map cannot cover: a
     * handler that persisted or removed entities through a manager on ANOTHER
     * connection. Those changes are outside the message transaction, nothing flushes
     * them later in a worker, and the commit would drop them silently — so the message
     * fails (and rolls back) with the misconfiguration spelled out instead. Detection
     * is intentionally cheap: persist/remove schedule immediately; dirty updates of
     * managed entities would require computing changesets, too intrusive even for dev.
     *
     * @param list<string> $flushedManagerNames
     */
    private function assertNoPendingChangesOutsideTransaction(array $flushedManagerNames, string $transportName): void
    {
        if (!$this->debug || !$this->flushEntityManagers || null === $this->doctrine) {
            return;
        }

        foreach ($this->doctrine->getManagers() as $name => $manager) {
            if (\in_array($name, $flushedManagerNames, true) || !$manager instanceof EntityManagerInterface || !$manager->isOpen()) {
                continue;
            }

            $unitOfWork = $manager->getUnitOfWork();
            if ([] === $unitOfWork->getScheduledEntityInsertions()
                && [] === $unitOfWork->getScheduledEntityUpdates()
                && [] === $unitOfWork->getScheduledEntityDeletions()) {
                continue;
            }

            throw new \LogicException(\sprintf('A handler of a message from transport "%s" left scheduled changes in entity manager "%s", which runs on a different connection than the message transaction — committing would silently drop them. Move the writes to an entity manager on the transaction\'s connection, or flush "%s" explicitly in the handler.', $transportName, $name, $name));
        }
    }

    /**
     * @return array{0: DbalConnection, 1: list<string>}|null the connection to wrap the handlers in, and the entity manager names to flush at the boundary
     */
    private function resolveMappedTransaction(Envelope $envelope, string $transportName): ?array
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
                // The explicitly mapped manager is flushed even when absent from the
                // compiled map — the mapping IS the configuration; managers sharing
                // its connection join it through the map.
                $connectionName = $this->connectionNameOfManager($mapped);

                return [$manager->getConnection(), null !== $connectionName ? $this->connectionEntityManagers[$connectionName] : [$mapped]];
            }
        }
        if (\array_key_exists($mapped, $this->doctrine->getConnectionNames())) {
            $connection = $this->doctrine->getConnection($mapped);
            if ($connection instanceof DbalConnection) {
                return [$connection, $this->connectionEntityManagers[$mapped] ?? []];
            }
        }

        throw new UnrecoverableMessageHandlingException(\sprintf('orm_mappings maps queue "%s" to "%s", which is neither a Doctrine entity manager nor a DBAL connection name.', $queueName ?? '', $mapped));
    }

    private function connectionNameOfManager(string $managerName): ?string
    {
        foreach ($this->connectionEntityManagers as $connectionName => $managerNames) {
            if (\in_array($managerName, $managerNames, true)) {
                return $connectionName;
            }
        }

        return null;
    }
}
