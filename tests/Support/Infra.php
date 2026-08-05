<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Support;

use PhpAmqpLib\Connection\AMQPStreamConnection;
use PHPUnit\Framework\Assert;

/**
 * Access to the local development infrastructure used by integration tests.
 *
 * Every require*() method returns a live connection or skips the calling test
 * when the service is unreachable, so integration suites degrade gracefully on
 * machines without the dev containers.
 */
final class Infra
{
    public const int REDIS_TEST_DATABASE = 15;

    private function __construct()
    {
    }

    public static function requirePostgres(): \PDO
    {
        $dsn = \sprintf(
            'pgsql:host=%s;port=%s;dbname=%s',
            self::env('MWF_TEST_PG_HOST', '127.0.0.1'),
            self::env('MWF_TEST_PG_PORT', '5432'),
            self::env('MWF_TEST_PG_DBNAME', 'mwf_test'),
        );

        try {
            return new \PDO(
                $dsn,
                self::env('MWF_TEST_PG_USER', 'test'),
                self::env('MWF_TEST_PG_PASSWORD', 'test'),
                [\PDO::ATTR_TIMEOUT => 2, \PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION],
            );
        } catch (\Throwable $e) {
            Assert::markTestSkipped(\sprintf('PostgreSQL is not available (%s): %s', $dsn, $e->getMessage()));
        }
    }

    public static function requireRedis(): \Redis
    {
        $host = self::env('MWF_TEST_REDIS_HOST', '127.0.0.1');
        $port = (int) self::env('MWF_TEST_REDIS_PORT', '6379');

        try {
            $redis = new \Redis();
            $redis->connect($host, $port, 2.0);

            $auth = self::env('MWF_TEST_REDIS_AUTH', '');
            if ('' !== $auth) {
                $redis->auth($auth);
            }

            $redis->select(self::REDIS_TEST_DATABASE);

            return $redis;
        } catch (\Throwable $e) {
            Assert::markTestSkipped(\sprintf('Redis is not available (%s:%d): %s', $host, $port, $e->getMessage()));
        }
    }

    public static function requireAmqp(): AMQPStreamConnection
    {
        $dsn = self::amqpDsn();
        $parts = parse_url($dsn);

        if (false === $parts || !isset($parts['host'])) {
            Assert::markTestSkipped(\sprintf('Invalid MWF_TEST_AMQP_DSN: "%s"', $dsn));
        }

        try {
            return new AMQPStreamConnection(
                $parts['host'],
                $parts['port'] ?? 5672,
                $parts['user'] ?? 'guest',
                $parts['pass'] ?? 'guest',
                '/' === ($parts['path'] ?? '') || !isset($parts['path']) ? '/' : urldecode(ltrim($parts['path'], '/')),
                connection_timeout: 3.0,
            );
        } catch (\Throwable $e) {
            Assert::markTestSkipped(\sprintf('RabbitMQ is not available (%s): %s', $dsn, $e->getMessage()));
        }
    }

    public static function amqpDsn(): string
    {
        return self::env('MWF_TEST_AMQP_DSN', 'phpamqplib://guest:guest@127.0.0.1:5672');
    }

    private static function env(string $name, string $default): string
    {
        $value = $_ENV[$name] ?? $_SERVER[$name] ?? getenv($name);

        return \is_string($value) && '' !== $value ? $value : $default;
    }
}
