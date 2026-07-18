<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Unit\Messenger;

use Kraz\MessengerWorkflow\Application\QueryInterface;
use Kraz\MessengerWorkflow\Messenger\QueryBus;
use Kraz\MessengerWorkflow\Messenger\Stamp\AsyncMessageStamp;
use Kraz\MessengerWorkflow\Messenger\Stamp\AwaitMessageResultStamp;
use Kraz\MessengerWorkflow\Messenger\Stamp\MessageIdStamp;
use Kraz\MessengerWorkflow\Messenger\Stamp\QueryStamp;
use Kraz\MessengerWorkflow\Tests\Fixture\Message\TestQuery;
use Kraz\MessengerWorkflow\Tests\Support\SpyMessageBus;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\HandlerFailedException;
use Symfony\Component\Messenger\Exception\LogicException;
use Symfony\Component\Messenger\Stamp\HandledStamp;
use Symfony\Component\Uid\Uuid;
use Symfony\Component\Uid\UuidV7;

final class QueryBusTest extends TestCase
{
    public function testRejectsMessagesNotImplementingQueryInterface(): void
    {
        $bus = new QueryBus(new SpyMessageBus());

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('Invalid query message');

        $bus->ask(new \stdClass());
    }

    public function testAskReturnsTheSingleHandlerResult(): void
    {
        $spy = new SpyMessageBus(static fn (Envelope $e) => $e->with(new HandledStamp('the-result', 'h')));
        $bus = new QueryBus($spy);

        self::assertSame('the-result', $bus->ask(new TestQuery()));

        $stamp = $spy->lastEnvelope()->last(QueryStamp::class);
        self::assertNotNull($stamp);
        self::assertInstanceOf(UuidV7::class, Uuid::fromString($stamp->getMessageId()));
        self::assertSame('queries.internal.Kraz', $stamp->getRoutingKey());
    }

    public function testAskFailsWhenHandledZeroTimes(): void
    {
        $bus = new QueryBus(new SpyMessageBus());

        $this->expectException(LogicException::class);
        $this->expectExceptionMessageMatches('/handled zero times/');

        $bus->ask(new TestQuery());
    }

    public function testAskFailsWhenHandledMultipleTimes(): void
    {
        $spy = new SpyMessageBus(static fn (Envelope $e) => $e
            ->with(new HandledStamp('a', 'one'))
            ->with(new HandledStamp('b', 'two')));
        $bus = new QueryBus($spy);

        $this->expectException(LogicException::class);
        $this->expectExceptionMessageMatches('/handled multiple times/');

        $bus->ask(new TestQuery());
    }

    public function testAskUnwrapsHandlerFailedException(): void
    {
        $spy = new SpyMessageBus(static function (Envelope $e): Envelope {
            throw new HandlerFailedException($e, [new \DomainException('nope')]);
        });
        $bus = new QueryBus($spy);

        $this->expectException(\DomainException::class);

        $bus->ask(new TestQuery());
    }

    public function testAskAsyncReturnsTaskId(): void
    {
        $spy = new SpyMessageBus(static fn (Envelope $e) => $e
            ->with(new MessageIdStamp($e->last(QueryStamp::class)->getMessageId())));
        $bus = new QueryBus($spy);

        $taskId = $bus->askAsync(new TestQuery());

        self::assertInstanceOf(UuidV7::class, Uuid::fromString($taskId));
        self::assertNotNull($spy->lastEnvelope()->last(AsyncMessageStamp::class));
    }

    public function testAwaitReturnsResultForTaskId(): void
    {
        $spy = new SpyMessageBus(static fn (Envelope $e) => $e->with(new HandledStamp(['a' => 1], 'h')));
        $bus = new QueryBus($spy, awaitDefaultTimeout: 90);

        $result = $bus->await('0197fa30-0000-7000-8000-000000000001');

        self::assertSame(['a' => 1], $result);
        $envelope = $spy->lastEnvelope();
        self::assertInstanceOf(QueryInterface::class, $envelope->getMessage());
        self::assertSame('0197fa30-0000-7000-8000-000000000001', $envelope->last(MessageIdStamp::class)?->getMessageId());
        self::assertTrue($envelope->last(AwaitMessageResultStamp::class)?->isDeferred());
    }
}
