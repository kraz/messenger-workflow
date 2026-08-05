<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Integration\Infrastructure;

use Kraz\MessengerWorkflow\Tests\Support\Infra;
use PHPUnit\Framework\Attributes\Group;
use PHPUnit\Framework\Attributes\RequiresPhpExtension;
use PHPUnit\Framework\TestCase;

#[Group('redis')]
#[RequiresPhpExtension('redis')]
final class RedisSmokeTest extends TestCase
{
    public function testRoundTrip(): void
    {
        $redis = Infra::requireRedis();
        $key = 'mwf:smoke:'.bin2hex(random_bytes(8));

        try {
            self::assertTrue($redis->setex($key, 10, 'ping'));
            self::assertSame('ping', $redis->get($key));
        } finally {
            $redis->del($key);
            $redis->close();
        }
    }
}
