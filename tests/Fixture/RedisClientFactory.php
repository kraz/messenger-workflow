<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Fixture;

final class RedisClientFactory
{
    public static function create(): \Redis
    {
        $redis = new \Redis();
        $redis->connect(
            \is_string($_ENV['MWF_TEST_REDIS_HOST'] ?? null) ? $_ENV['MWF_TEST_REDIS_HOST'] : '127.0.0.1',
            is_numeric($_ENV['MWF_TEST_REDIS_PORT'] ?? null) ? (int) $_ENV['MWF_TEST_REDIS_PORT'] : 6379,
            2.0,
        );

        $auth = $_ENV['MWF_TEST_REDIS_AUTH'] ?? null;
        if (\is_string($auth) && '' !== $auth) {
            $redis->auth($auth);
        }

        // Keep test keys away from real databases.
        $redis->select(15);

        return $redis;
    }
}
