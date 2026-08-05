<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Functional;

use Kraz\MessengerWorkflow\Application\Task\ResultStorageInterface;
use Kraz\MessengerWorkflow\Infrastructure\Task\RedisResultStorage;
use Kraz\MessengerWorkflow\Tests\Support\WorkflowKernelTestCase;
use Kraz\MessengerWorkflow\Tests\TestKernel\RedisResultStorageKernel;
use PHPUnit\Framework\Attributes\Group;
use Symfony\Component\Messenger\Exception\TransportException;

#[Group('redis')]
final class RedisResultStorageAutowiringTest extends WorkflowKernelTestCase
{
    protected static function getKernelClass(): string
    {
        return RedisResultStorageKernel::class;
    }

    public function testRedisProviderUsesTheConfiguredClientService(): void
    {
        self::bootKernel();

        $storage = self::getContainer()->get(ResultStorageInterface::class);
        self::assertInstanceOf(RedisResultStorage::class, $storage);

        // Round-trip through the container-wired storage (connects to the dev Redis).
        try {
            $storage->write('autowire-check', 'ok');
            self::assertSame('ok', $storage->await('autowire-check', 1));
        } catch (TransportException $e) {
            self::markTestSkipped('Redis is not reachable: '.$e->getMessage());
        }
    }
}
