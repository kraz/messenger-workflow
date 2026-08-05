<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Support;

use Doctrine\DBAL\Connection as DBALConnection;
use Doctrine\DBAL\DriverManager;

final class PostgresDbal
{
    private function __construct()
    {
    }

    public static function createConnection(): DBALConnection
    {
        $env = static fn (string $name, string $default): string => \is_string($_ENV[$name] ?? null) && '' !== $_ENV[$name] ? $_ENV[$name] : $default;

        return DriverManager::getConnection([
            'driver' => 'pdo_pgsql',
            'host' => $env('MWF_TEST_PG_HOST', '127.0.0.1'),
            'port' => (int) $env('MWF_TEST_PG_PORT', '5432'),
            'user' => $env('MWF_TEST_PG_USER', 'test'),
            'password' => $env('MWF_TEST_PG_PASSWORD', 'test'),
            'dbname' => $env('MWF_TEST_PG_DBNAME', 'mwf_test'),
        ]);
    }
}
