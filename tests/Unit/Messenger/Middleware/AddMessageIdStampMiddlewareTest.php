<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Unit\Messenger\Middleware;

use Kraz\MessengerWorkflow\Messenger\Middleware\AddMessageIdStampMiddleware;
use Kraz\MessengerWorkflow\Messenger\Stamp\CommandStamp;
use Kraz\MessengerWorkflow\Messenger\Stamp\EventStamp;
use Kraz\MessengerWorkflow\Messenger\Stamp\MessageIdStamp;
use Kraz\MessengerWorkflow\Messenger\Stamp\QueryStamp;
use Kraz\MessengerWorkflow\Tests\Fixture\Message\TestCommand;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Middleware\StackMiddleware;
use Symfony\Component\Uid\Uuid;
use Symfony\Component\Uid\UuidV7;

final class AddMessageIdStampMiddlewareTest extends TestCase
{
    private AddMessageIdStampMiddleware $middleware;

    protected function setUp(): void
    {
        $this->middleware = new AddMessageIdStampMiddleware();
    }

    private function handle(Envelope $envelope): Envelope
    {
        return $this->middleware->handle($envelope, new StackMiddleware());
    }

    public function testGeneratesUuidV7WhenNoStampsPresent(): void
    {
        // Spec: "Dispatching a task returns a globally unique ID (UUID v7)"
        $result = $this->handle(new Envelope(new TestCommand()));

        $stamp = $result->last(MessageIdStamp::class);
        self::assertNotNull($stamp);
        self::assertInstanceOf(UuidV7::class, Uuid::fromString($stamp->getMessageId()));
    }

    public function testReusesEventStampMessageId(): void
    {
        $result = $this->handle(new Envelope(new TestCommand(), [new EventStamp('id-event', 'rk')]));

        self::assertSame('id-event', $result->last(MessageIdStamp::class)?->getMessageId());
    }

    public function testReusesQueryStampMessageId(): void
    {
        $result = $this->handle(new Envelope(new TestCommand(), [new QueryStamp('id-query', 'rk')]));

        self::assertSame('id-query', $result->last(MessageIdStamp::class)?->getMessageId());
    }

    public function testReusesCommandStampMessageId(): void
    {
        $result = $this->handle(new Envelope(new TestCommand(), [new CommandStamp('id-cmd', 'rk')]));

        self::assertSame('id-cmd', $result->last(MessageIdStamp::class)?->getMessageId());
    }

    public function testExistingMessageIdStampIsPreserved(): void
    {
        $result = $this->handle(new Envelope(new TestCommand(), [
            new MessageIdStamp('id-existing'),
            new CommandStamp('id-cmd', 'rk'),
        ]));

        self::assertSame('id-existing', $result->last(MessageIdStamp::class)?->getMessageId());
        self::assertCount(1, $result->all(MessageIdStamp::class));
    }
}
