<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Unit\Infrastructure\Task;

use Kraz\MessengerWorkflow\Application\Exception\TaskFailedException;
use Kraz\MessengerWorkflow\Application\Task\Exception\ResultStorageWaitTimeoutException;
use Kraz\MessengerWorkflow\Application\Task\ResultStoragePayload;
use Kraz\MessengerWorkflow\Infrastructure\Task\InMemoryResultStorage;
use PHPUnit\Framework\TestCase;

final class InMemoryResultStorageTest extends TestCase
{
    private InMemoryResultStorage $storage;

    protected function setUp(): void
    {
        $this->storage = new InMemoryResultStorage();
    }

    public function testWriteAndAwaitRoundTrip(): void
    {
        $this->storage->write('id-1', 42);

        self::assertSame(42, $this->storage->await('id-1', 1));
    }

    public function testPayloadValueIsUnwrapped(): void
    {
        $this->storage->write('id-1', new ResultStoragePayload(['a' => 1]));

        self::assertSame(['a' => 1], $this->storage->await('id-1', 1));
    }

    public function testMissingResultThrowsImmediately(): void
    {
        $start = microtime(true);

        try {
            $this->storage->await('unknown', 30);
            self::fail('Expected ResultStorageWaitTimeoutException');
        } catch (ResultStorageWaitTimeoutException) {
            // Single-process storage: nothing can arrive while blocked — no waiting.
            self::assertLessThan(1.0, microtime(true) - $start);
        }
    }

    public function testErrorPayloadThrowsTaskFailedException(): void
    {
        $this->storage->writeError('id-err', 'It broke', 7, 'App\SomeCommand', '#0 frame');

        try {
            $this->storage->await('id-err', 1);
            self::fail('Expected TaskFailedException');
        } catch (TaskFailedException $exception) {
            self::assertSame('It broke', $exception->getMessage());
            self::assertSame(7, $exception->getCode());
            self::assertSame('App\SomeCommand', $exception->getTaskClass());
            self::assertSame('#0 frame', $exception->getTaskTrace());
        }
    }

    public function testResetClearsStoredResults(): void
    {
        $this->storage->write('id-1', 'x');
        $this->storage->reset();

        $this->expectException(ResultStorageWaitTimeoutException::class);
        $this->storage->await('id-1', 1);
    }
}
