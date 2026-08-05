<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Unit\Application\Exception;

use Kraz\MessengerWorkflow\Application\Exception\TaskFailedException;
use PHPUnit\Framework\TestCase;

final class TaskFailedExceptionTest extends TestCase
{
    public function testCarriesTaskClassAndTrace(): void
    {
        $previous = new \RuntimeException('boom');
        $exception = new TaskFailedException('Task failed', 42, 'App\SomeCommand', "#0 trace-line\n#1 other", $previous);

        self::assertSame('Task failed', $exception->getMessage());
        self::assertSame(42, $exception->getCode());
        self::assertSame('App\SomeCommand', $exception->getTaskClass());
        self::assertSame("#0 trace-line\n#1 other", $exception->getTaskTrace());
        self::assertSame($previous, $exception->getPrevious());
    }

    public function testDefaultsAreNull(): void
    {
        $exception = new TaskFailedException();

        self::assertNull($exception->getTaskClass());
        self::assertNull($exception->getTaskTrace());
        self::assertStringNotContainsString('Task trace:', (string) $exception);
    }

    public function testToStringAppendsTaskTrace(): void
    {
        $exception = new TaskFailedException('failed', 0, null, '#0 remote-frame');

        self::assertStringContainsString('Task trace: #0 remote-frame', (string) $exception);
    }
}
