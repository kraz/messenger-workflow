<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Unit\Messenger;

use Kraz\MessengerWorkflow\Application\CommandInterface;
use Kraz\MessengerWorkflow\Messenger\CommandBus;
use Kraz\MessengerWorkflow\Messenger\Stamp\AsyncMessageStamp;
use Kraz\MessengerWorkflow\Messenger\Stamp\AwaitMessageResultStamp;
use Kraz\MessengerWorkflow\Messenger\Stamp\CommandStamp;
use Kraz\MessengerWorkflow\Messenger\Stamp\MessageIdStamp;
use Kraz\MessengerWorkflow\Tests\Fixture\Message\TestCommand;
use Kraz\MessengerWorkflow\Tests\Support\SpyMessageBus;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\HandlerFailedException;
use Symfony\Component\Messenger\Exception\LogicException;
use Symfony\Component\Messenger\Stamp\HandledStamp;
use Symfony\Component\Uid\Uuid;
use Symfony\Component\Uid\UuidV7;

final class CommandBusTest extends TestCase
{
    public function testRejectsMessagesNotImplementingCommandInterface(): void
    {
        $bus = new CommandBus(new SpyMessageBus());

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('Invalid command message');

        $bus->dispatch(new \stdClass());
    }

    public function testDispatchAddsCommandStampWithUuidV7AndRoutingKey(): void
    {
        $spy = new SpyMessageBus(static fn (Envelope $e) => $e->with(new HandledStamp(null, 'h')));
        $bus = new CommandBus($spy);

        $bus->dispatch(new TestCommand());

        $stamp = $spy->lastEnvelope()->last(CommandStamp::class);
        self::assertNotNull($stamp);
        self::assertInstanceOf(UuidV7::class, Uuid::fromString($stamp->getMessageId()));
        self::assertSame('commands.internal.Kraz', $stamp->getRoutingKey());
    }

    public function testDispatchUsesGivenTimeoutOrDefault(): void
    {
        $spy = new SpyMessageBus(static fn (Envelope $e) => $e->with(new HandledStamp(null, 'h')));
        $bus = new CommandBus($spy, awaitDefaultTimeout: 123);

        $bus->dispatch(new TestCommand());
        self::assertSame(123, $spy->lastEnvelope()->last(AwaitMessageResultStamp::class)?->getTimeout());

        $bus->dispatch(new TestCommand(), timeout: 7);
        self::assertSame(7, $spy->lastEnvelope()->last(AwaitMessageResultStamp::class)?->getTimeout());
    }

    public function testDispatchFailsWhenHandledZeroTimes(): void
    {
        // Spec: Commands/Tasks have exactly one handler
        $bus = new CommandBus(new SpyMessageBus());

        $this->expectException(LogicException::class);
        $this->expectExceptionMessageMatches('/handled zero times/');

        $bus->dispatch(new TestCommand());
    }

    public function testDispatchFailsWhenHandledMultipleTimes(): void
    {
        $spy = new SpyMessageBus(static fn (Envelope $e) => $e
            ->with(new HandledStamp(null, 'one'))
            ->with(new HandledStamp(null, 'two')));
        $bus = new CommandBus($spy);

        $this->expectException(LogicException::class);
        $this->expectExceptionMessageMatches('/handled multiple times/');

        $bus->dispatch(new TestCommand());
    }

    public function testDispatchUnwrapsHandlerFailedException(): void
    {
        $spy = new SpyMessageBus(static function (Envelope $e): Envelope {
            throw new HandlerFailedException($e, [new \DomainException('boom', 42)]);
        });
        $bus = new CommandBus($spy);

        $this->expectException(\DomainException::class);
        $this->expectExceptionMessage('boom');

        $bus->dispatch(new TestCommand());
    }

    public function testDispatchAsyncReturnsTaskIdFromMessageIdStamp(): void
    {
        // Simulate AddMessageIdStampMiddleware copying the id from the CommandStamp
        $spy = new SpyMessageBus(static fn (Envelope $e) => $e
            ->with(new MessageIdStamp($e->last(CommandStamp::class)->getMessageId())));
        $bus = new CommandBus($spy);

        $taskId = $bus->dispatchAsync(new TestCommand());

        self::assertInstanceOf(UuidV7::class, Uuid::fromString($taskId));
        self::assertNotNull($spy->lastEnvelope()->last(AsyncMessageStamp::class));
    }

    public function testDispatchAsyncFailsWithoutMessageIdStamp(): void
    {
        $bus = new CommandBus(new SpyMessageBus());

        $this->expectException(LogicException::class);

        $bus->dispatchAsync(new TestCommand());
    }

    public function testAwaitDispatchesPlaceholderCommandWithDeferredAwaitStamp(): void
    {
        $spy = new SpyMessageBus(static fn (Envelope $e) => $e->with(new HandledStamp(null, 'h')));
        $bus = new CommandBus($spy, awaitDefaultTimeout: 55);

        $bus->await('0197fa30-0000-7000-8000-000000000000');

        $envelope = $spy->lastEnvelope();
        self::assertInstanceOf(CommandInterface::class, $envelope->getMessage());
        self::assertSame('0197fa30-0000-7000-8000-000000000000', $envelope->last(MessageIdStamp::class)?->getMessageId());
        $await = $envelope->last(AwaitMessageResultStamp::class);
        self::assertNotNull($await);
        self::assertTrue($await->isDeferred());
        self::assertSame(55, $await->getTimeout());
    }
}
