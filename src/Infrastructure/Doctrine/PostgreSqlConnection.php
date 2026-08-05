<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Infrastructure\Doctrine;

use Pdo\Pgsql;
use Symfony\Component\Messenger\Exception\TransportException;

/**
 * Uses PostgreSQL LISTEN/NOTIFY to push messages to workers.
 *
 * Blocking on the notification can either happen inside {@see self::get()} (fallback used
 * when no external listener is wired up) or be delegated to
 * {@see \Kraz\MessengerWorkflow\Infrastructure\Messenger\EventListener\PostgreSqlNotifyOnIdleListener},
 * which waits only when the worker is idle. The latter lets a single worker consume from
 * several queues without one queue's blocking wait starving the others.
 */
class PostgreSqlConnection extends Connection
{
    /**
     * * check_delayed_interval: The interval to check for messages anyway, in milliseconds. Set to 0 to disable checks. Default: 60000 (1 minute)
     * * get_notify_timeout: The maximum time to wait for a NOTIFY, in milliseconds. Default: 60000 (1 minute).
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

        // Fallback: when no external listener handles LISTEN/NOTIFY, block here
        // until a notification arrives or the timeout expires.

        // This is secure because the table name must be a valid identifier:
        // https://www.postgresql.org/docs/current/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS
        $this->executeStatement(\sprintf('LISTEN "%s"', $this->tableName));
        $this->listening = true;

        // Wait at most until the next check_delayed_interval boundary so the worker re-polls
        // even if no NOTIFY arrives (e.g. a notification missed between emptying the queue
        // and listening).
        $checkDelayedInterval = $this->getCheckDelayedIntervalMs();
        $timeout = $checkDelayedInterval - (microtime(true) * 1000 - $this->queueEmptiedAt);
        $notifyTimeout = $this->getNotifyTimeoutMs();
        $timeout = max(0, (int) ceil(min(0 !== $notifyTimeout ? (float) $notifyTimeout : $timeout, $timeout)));

        $notification = $this->getNotify($timeout);
        if (
            // no notification, or a notification for another table
            (false === $notification || ($notification['message'] ?? null) !== $this->tableName)
            && (microtime(true) * 1000 - $this->queueEmptiedAt < $checkDelayedInterval)
        ) {
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
