<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Unit\Infrastructure\Messenger\Middleware;

use Kraz\MessengerWorkflow\Infrastructure\Messenger\Middleware\MessageIdMiddleware;
use Kraz\MessengerWorkflow\Infrastructure\Messenger\Stamp\MessageIdStamp;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Middleware\StackMiddleware;
use Symfony\Component\Uid\Uuid;

final class MessageIdMiddlewareTest extends TestCase
{
    public function testAddsAUuidV7MessageIdWhenMissing(): void
    {
        $middleware = new MessageIdMiddleware();

        $envelope = $middleware->handle(new Envelope(new \stdClass()), new StackMiddleware());

        $stamp = $envelope->last(MessageIdStamp::class);
        self::assertNotNull($stamp);
        self::assertTrue(Uuid::isValid($stamp->getMessageId()));
        self::assertInstanceOf(\Symfony\Component\Uid\UuidV7::class, Uuid::fromString($stamp->getMessageId()));
    }

    public function testKeepsAnExistingMessageId(): void
    {
        $middleware = new MessageIdMiddleware();
        $existing = new MessageIdStamp('0198a000-0000-7000-8000-000000000000');

        $envelope = $middleware->handle(new Envelope(new \stdClass(), [$existing]), new StackMiddleware());

        self::assertCount(1, $envelope->all(MessageIdStamp::class));
        self::assertSame($existing, $envelope->last(MessageIdStamp::class));
    }
}
