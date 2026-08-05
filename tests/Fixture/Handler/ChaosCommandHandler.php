<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Fixture\Handler;

use Contracts\Chaos\Command\ChaosCommand;
use Doctrine\DBAL\Connection;
use Kraz\MessengerWorkflow\Application\Attribute\AsCommandHandler;

/**
 * Chaos-test handler: writes to the chaos_handled application table so
 * transactional atomicity is observable from a second database session. The
 * "kill-db-once" payload terminates its own PostgreSQL backend on the first attempt
 * AFTER writing — simulating the database going away mid-handle.
 */
#[AsCommandHandler]
final class ChaosCommandHandler
{
    /**
     * @var array<string, int>
     */
    public static array $attempts = [];

    public static function reset(): void
    {
        self::$attempts = [];
    }

    public static function attempts(string $payload): int
    {
        return self::$attempts[$payload] ?? 0;
    }

    public function __construct(private readonly Connection $postgres)
    {
    }

    public function __invoke(ChaosCommand $command): void
    {
        $attempt = self::$attempts[$command->payload] = self::attempts($command->payload) + 1;

        $this->postgres->executeStatement(
            'INSERT INTO chaos_handled (payload) VALUES (?)',
            [$command->payload],
        );

        if ('kill-db-once' === $command->payload && 1 === $attempt) {
            // Kills this session's backend: the statement itself fails with a
            // connection-lost error and everything uncommitted is rolled back
            // server-side.
            $this->postgres->executeStatement('SELECT pg_terminate_backend(pg_backend_pid())');
        }
    }
}
