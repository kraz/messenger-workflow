<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Unit\Messenger\Transport;

use Kraz\MessengerWorkflow\Application\Exception\TaskFailedException;
use Kraz\MessengerWorkflow\Messenger\Transport\Exception\ResultStorageWaitTimeoutException;
use Kraz\MessengerWorkflow\Messenger\Transport\InMemoryResultStorage;
use PHPUnit\Framework\TestCase;

final class InMemoryResultStorageTest extends TestCase
{
    public function testWriteAndAwaitReturnsValue(): void
    {
        $storage = new InMemoryResultStorage();
        $storage->write('id-1', ['x' => 1]);

        self::assertSame(['x' => 1], $storage->await('id-1', 1));
    }

    public function testAwaitNullResultIsValid(): void
    {
        $storage = new InMemoryResultStorage();
        $storage->write('id-null', null);

        self::assertNull($storage->await('id-null', 1));
    }

    public function testAwaitMissingResultThrowsTimeout(): void
    {
        $storage = new InMemoryResultStorage();

        $this->expectException(ResultStorageWaitTimeoutException::class);

        $storage->await('missing', 1);
    }

    public function testWriteErrorProducesTaskFailedException(): void
    {
        $storage = new InMemoryResultStorage();
        $storage->writeError('id-err', 'it broke', 42, \DomainException::class, "#0 trace");

        try {
            $storage->await('id-err', 1);
            self::fail('Expected TaskFailedException');
        } catch (TaskFailedException $e) {
            self::assertSame('it broke', $e->getMessage());
            self::assertSame(42, $e->getCode());
            self::assertSame(\DomainException::class, $e->getTaskClass());
            self::assertSame('#0 trace', $e->getTaskTrace());
        }
    }

    public function testResetClearsStoredResults(): void
    {
        $storage = new InMemoryResultStorage();
        $storage->write('id-1', 'v');
        $storage->reset();

        $this->expectException(ResultStorageWaitTimeoutException::class);
        $storage->await('id-1', 1);
    }
}
