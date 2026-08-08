<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Infrastructure\Doctrine;

use Pdo\Pgsql;
use Symfony\Component\Messenger\Exception\TransportException;

/**
 * Uses PostgreSQL LISTEN/NOTIFY to push messages to workers.
 *
 * All blocking on the notification is delegated to
 * {@see \Kraz\MessengerWorkflow\Infrastructure\Messenger\EventListener\PostgreSqlNotifyOnIdleListener},
 * which waits only while the worker is idle and knows how long the worker can afford to
 * block (sibling transports, worker sleep, --time-limit). get() itself never waits: it is
 * called for a single receiver of a worker that may consume several transports, so a wait
 * here would starve the sibling receivers and override the worker's polling rate. When no
 * listener is wired up, the fallback in get() only collects notifications that already
 * arrived.
 */
class PostgreSqlConnection extends Connection
{
    /**
     * * check_delayed_interval: The interval to check for messages anyway, in milliseconds. Set to 0 to disable checks. Default: 60000 (1 minute)
     * * get_notify_timeout: The maximum time PostgreSqlNotifyOnIdleListener waits for a NOTIFY while the worker
     *                       is idle, in milliseconds. Set to 0 to disable waiting. Default: 60000 (1 minute).
     */
    protected const array DEFAULT_OPTIONS = parent::DEFAULT_OPTIONS + [
        'check_delayed_interval' => 60000,
        'get_notify_timeout' => 60000,
    ];

    private bool $listening = false;
    private bool $notifyHandledExternally = false;

    /**
     * @return array<array-key, mixed>
     */
    public function __serialize(): array
    {
        throw new \BadMethodCallException('Cannot serialize '.__CLASS__);
    }

    /**
     * @param array<array-key, mixed> $data
     */
    public function __unserialize(array $data): void
    {
        throw new \BadMethodCallException('Cannot unserialize '.__CLASS__);
    }

    public function __destruct()
    {
        $this->unlisten();
    }

    public function isListening(): bool
    {
        return $this->listening;
    }

    public function getNotifyTimeoutMs(): int
    {
        return is_numeric($this->configuration['get_notify_timeout'] ?? null) ? (int) $this->configuration['get_notify_timeout'] : 60000;
    }

    public function getCheckDelayedIntervalMs(): int
    {
        return is_numeric($this->configuration['check_delayed_interval'] ?? null) ? (int) $this->configuration['check_delayed_interval'] : 60000;
    }

    public function reset(): void
    {
        parent::reset();
        $this->unlisten();
    }

    public function get(int $fetchSize = 1): ?array
    {
        if ($this->notifyHandledExternally || null === $this->queueEmptiedAt) {
            return parent::get($fetchSize);
        }

        // Fallback: when no external listener handles LISTEN/NOTIFY, only collect the
        // notifications that already arrived. Waiting for one is up to
        // PostgreSqlNotifyOnIdleListener, which knows how long the worker can afford to
        // block, while get() is called for a single receiver of that worker.

        // This is secure because the table name must be a valid identifier:
        // https://www.postgresql.org/docs/current/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS
        $this->executeStatement(\sprintf('LISTEN "%s"', $this->tableName));
        $this->listening = true;

        $notification = $this->getNotify(0);
        if (
            // no notification, or a notification for another table
            (false === $notification || ($notification['message'] ?? null) !== $this->tableName)
            // the check_delayed_interval re-poll boundary is not reached yet
            && (microtime(true) * 1000 - $this->queueEmptiedAt < $this->getCheckDelayedIntervalMs())
        ) {
            usleep(1000);

            return null;
        }

        return parent::get($fetchSize);
    }

    /**
     * Registers a LISTEN on the PostgreSQL connection for the configured table.
     *
     * When called, also disables the internal LISTEN/NOTIFY blocking in get(),
     * assuming an external listener (e.g. PostgreSqlNotifyOnIdleListener) handles it.
     *
     * Safe to call multiple times; PostgreSQL ignores duplicate LISTEN for the same channel.
     *
     * @param bool $registerOnDatabase Whether to execute the SQL LISTEN command. When false, only marks
     *                                 get() as externally handled without registering on the database. This
     *                                 avoids accumulating unread notifications on connections that will never
     *                                 call waitForNotify().
     */
    public function listen(bool $registerOnDatabase = true): void
    {
        if ($registerOnDatabase) {
            // This is secure because the table name must be a valid identifier:
            // https://www.postgresql.org/docs/current/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS
            $this->executeStatement(\sprintf('LISTEN "%s"', $this->tableName));
            $this->listening = true;
        }
        $this->notifyHandledExternally = true;
    }

    /**
     * Blocks until a PostgreSQL NOTIFY is received or the timeout expires.
     *
     * Automatically registers a LISTEN before waiting to handle reconnections.
     *
     * @param int $timeoutMs The maximum time to wait in milliseconds
     *
     * @return bool True if a notification was received, false on timeout
     */
    public function waitForNotify(int $timeoutMs): bool
    {
        $this->listen();

        return false !== $this->getNotify($timeoutMs);
    }

    private function unlisten(): void
    {
        if (!$this->listening) {
            return;
        }

        $this->executeStatement(\sprintf('UNLISTEN "%s"', $this->tableName));
        $this->listening = false;
    }

    /**
     * @return array{message?: string, pid?: int, payload?: string}|false
     */
    private function getNotify(int $timeoutMs): array|false
    {
        $nativeConnection = $this->driverConnection->getNativeConnection();
        if (!$nativeConnection instanceof Pgsql) {
            throw new TransportException(\sprintf('Expected the native connection to be "%s", got "%s".', Pgsql::class, get_debug_type($nativeConnection)));
        }

        /** @var array{message?: string, pid?: int, payload?: string}|false */
        return $nativeConnection->getNotify(\PDO::FETCH_ASSOC, $timeoutMs);
    }
}
