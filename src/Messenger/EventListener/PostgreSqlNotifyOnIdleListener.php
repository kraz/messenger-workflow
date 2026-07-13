<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Messenger\EventListener;

use Kraz\MessengerWorkflow\Messenger\Doctrine\PostgreSqlConnection;
use Psr\Clock\ClockInterface;
use Psr\Log\LoggerInterface;
use Symfony\Component\EventDispatcher\EventSubscriberInterface;
use Symfony\Component\Messenger\Event\WorkerRunningEvent;
use Symfony\Component\Messenger\Event\WorkerStartedEvent;

/**
 * When the worker is idle, blocks on PostgreSQL LISTEN/NOTIFY instead of polling. This allows instant
 * wake-up when a new message arrives while properly supporting workers that consume from multiple queues.
 *
 * Ported from Symfony 8.1's {@see \Symfony\Component\Messenger\Bridge\Doctrine\EventListener\PostgreSqlNotifyOnIdleListener}
 * for the Kraz\MessengerWorkflow inbox/outbox transports. The delayed-message capping of the upstream listener
 * is intentionally omitted: the Kraz\MessengerWorkflow message tables have no "available_at"/"queue_name"
 * columns and the transports do not support delaying messages.
 */
class PostgreSqlNotifyOnIdleListener implements EventSubscriberInterface
{
    /** @var array<string, PostgreSqlConnection> */
    private array $connections = [];
    private ?PostgreSqlConnection $activeConnection = null;
    private ?float $deadline = null;
    private ?int $sleepCapMs = null;

    public function __construct(
        private ?LoggerInterface $logger = null,
        private ?ClockInterface $clock = null,
    ) {
    }

    /**
     * Registers a PostgreSQL connection candidate for LISTEN/NOTIFY.
     *
     * Called by the inbox/outbox transport factories during transport creation.
     */
    public function addConnection(string $transportName, PostgreSqlConnection $connection): void
    {
        $this->connections[$transportName] = $connection;
    }

    public function onWorkerStarted(WorkerStartedEvent $event): void
    {
        $this->activeConnection = null;
        $this->deadline = $event->getDeadline();

        $allTransportNames = $event->getWorker()->getMetadata()->getTransportNames();

        $matched = [];
        foreach ($allTransportNames as $transportName) {
            if ($connection = $this->connections[$transportName] ?? null) {
                $matched[$transportName] = $connection;
            }
        }

        // When non-PostgreSQL transports are also consumed, cap the NOTIFY wait to
        // the worker's sleep duration so those transports are still polled regularly.
        $this->sleepCapMs = \count($matched) < \count($allTransportNames) ? (int) ($event->getIdleTimeout() / 1000) : null;

        if (\count($matched) > 1) {
            $this->validateConnections($matched);
        }

        foreach ($matched as $connection) {
            // Only the first (active) connection executes LISTEN on the database; the others just mark get() as
            // externally handled to avoid accumulating unread notifications on connections that never call waitForNotify().
            $connection->listen(null === $this->activeConnection);
            $this->activeConnection ??= $connection;
        }
    }

    public function onWorkerRunning(WorkerRunningEvent $event): void
    {
        if (!$event->isWorkerIdle() || !$this->activeConnection) {
            return;
        }

        $config = $this->activeConnection->getConfiguration();

        if (0 >= $timeout = $config['get_notify_timeout']) {
            return;
        }

        $now = $this->clock?->now()->format('U.u') ?? microtime(true);

        // Cap by worker deadline (--time-limit) so the worker still stops on time.
        if (null !== $this->deadline) {
            $deadline = ($this->deadline - $now) * 1000;
            if (0 >= $timeout = min($timeout, $deadline)) {
                return;
            }
        }

        // Cap by sleep duration when non-PG transports are present to ensure they are still polled regularly.
        if (0 >= $timeout = (int) min($timeout, $this->sleepCapMs ?? $timeout)) {
            return;
        }

        $this->logger?->debug('Worker waiting for PostgreSQL LISTEN/NOTIFY wake-up.', ['timeout_ms' => $timeout]);

        $this->activeConnection->waitForNotify($timeout);
    }

    public static function getSubscribedEvents(): array
    {
        return [
            WorkerStartedEvent::class => 'onWorkerStarted',
            WorkerRunningEvent::class => 'onWorkerRunning',
        ];
    }

    /**
     * @param array<string, PostgreSqlConnection> $connections
     */
    private function validateConnections(array $connections): void
    {
        $referenceConfig = null;
        $referenceDriver = null;
        $referenceName = null;

        foreach ($connections as $transportName => $connection) {
            $config = $connection->getConfiguration();
            $driver = $connection->getDriverConnection();

            if (null === $referenceConfig) {
                $referenceConfig = $config;
                $referenceDriver = $driver;
                $referenceName = $transportName;
                continue;
            }

            if ($driver !== $referenceDriver) {
                throw new \LogicException(\sprintf('PostgreSQL transports "%s" and "%s" use different DBAL connections. When consuming from multiple PostgreSQL queues in one worker, all transports must share the same DBAL connection.', $referenceName, $transportName));
            }

            if ($config['table_name'] !== $referenceConfig['table_name']) {
                throw new \LogicException(\sprintf('PostgreSQL transports "%s" and "%s" use different table_name values ("%s" vs "%s"). When consuming from multiple PostgreSQL queues in one worker, all transports must use the same table.', $referenceName, $transportName, $referenceConfig['table_name'], $config['table_name']));
            }
        }
    }
}
