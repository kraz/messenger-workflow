<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Fixture;

final class RedisClientFactory
{
    public static function create(): \Redis
    {
        $redis = new \Redis();
        $redis->connect(
            $_ENV['MWF_TEST_REDIS_HOST'] ?? '127.0.0.1',
            (int) ($_ENV['MWF_TEST_REDIS_PORT'] ?? 6379),
            2.0,
        );
        $auth = $_ENV['MWF_TEST_REDIS_AUTH'] ?? null;
        if (null !== $auth && '' !== $auth) {
            $redis->auth($auth);
        }
        // keep test keys away from real databases
        $redis->select(15);

        return $redis;
    }
}
