<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Unit\Messenger\Middleware;

use Kraz\MessengerWorkflow\Messenger\Event\CommandCompletedNotification;
use Kraz\MessengerWorkflow\Messenger\Middleware\OutboxMiddleware;
use Kraz\MessengerWorkflow\Messenger\Transport\InMemoryResultStorage;
use Kraz\MessengerWorkflow\Tests\Fixture\Message\TestEvent;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Middleware\StackMiddleware;
use Symfony\Component\Messenger\Stamp\HandledStamp;
use Symfony\Component\Messenger\Stamp\ReceivedStamp;

final class OutboxMiddlewareTest extends TestCase
{
    private InMemoryResultStorage $storage;
    private OutboxMiddleware $middleware;

    protected function setUp(): void
    {
        $this->storage = new InMemoryResultStorage();
        $this->middleware = new OutboxMiddleware($this->storage);
    }

    public function testOutgoingMessagePassesThroughAndIsMarkedHandled(): void
    {
        $envelope = new Envelope(new TestEvent());

        $result = $this->middleware->handle($envelope, new StackMiddleware());

        self::assertSame('outbox', $result->last(HandledStamp::class)?->getHandlerName());
    }

    public function testReceivedCommandCompletedNotificationWritesResultToStorage(): void
    {
        $notification = new CommandCompletedNotification('cmd-1', ['done' => true]);
        $envelope = new Envelope($notification, [new ReceivedStamp('app_commands_notifier')]);

        $result = $this->middleware->handle($envelope, new StackMiddleware());

        self::assertSame('outbox', $result->last(HandledStamp::class)?->getHandlerName());
        self::assertSame(['done' => true], $this->storage->await('cmd-1', 1));
    }

    public function testReceivingAnyOtherMessageIsRejected(): void
    {
        // Spec: outbox.bus is internal — it must never process regular messages
        $envelope = new Envelope(new TestEvent(), [new ReceivedStamp('app_outbox')]);

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessageMatches('/not supposed to receive or handle messages/');

        $this->middleware->handle($envelope, new StackMiddleware());
    }
}
