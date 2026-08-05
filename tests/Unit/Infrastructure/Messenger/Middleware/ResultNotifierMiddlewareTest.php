<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Unit\Infrastructure\Messenger\Middleware;

use Contracts\Demo\Command\DoSomethingCommand;
use Kraz\MessengerWorkflow\Infrastructure\Messenger\CommandCompletedNotification;
use Kraz\MessengerWorkflow\Infrastructure\Messenger\Middleware\ResultNotifierMiddleware;
use Kraz\MessengerWorkflow\Infrastructure\Task\InMemoryResultStorage;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\LogicException;
use Symfony\Component\Messenger\Exception\UnrecoverableMessageHandlingException;
use Symfony\Component\Messenger\MessageBus;
use Symfony\Component\Messenger\Stamp\HandledStamp;
use Symfony\Component\Messenger\Stamp\ReceivedStamp;

final class ResultNotifierMiddlewareTest extends TestCase
{
    private InMemoryResultStorage $resultStorage;
    private MessageBus $bus;

    protected function setUp(): void
    {
        $this->resultStorage = new InMemoryResultStorage();
        $this->bus = new MessageBus([new ResultNotifierMiddleware($this->resultStorage)]);
    }

    public function testAConsumedNotificationIsPublishedToTheResultStorage(): void
    {
        $notification = CommandCompletedNotification::forResult('task-1', ['answer' => 42]);

        $envelope = $this->bus->dispatch(new Envelope($notification, [new ReceivedStamp('app_commands_notifier')]));

        self::assertSame(['answer' => 42], $this->resultStorage->await('task-1', 1));
        self::assertNotNull($envelope->last(HandledStamp::class));
    }

    public function testDirectDispatchIsRejected(): void
    {
        $this->expectException(LogicException::class);
        $this->expectExceptionMessageMatches('/dispatched directly/');

        $this->bus->dispatch(CommandCompletedNotification::forResult('task-1', null));
    }

    public function testForeignMessagesAreAPermanentFailure(): void
    {
        $this->expectException(UnrecoverableMessageHandlingException::class);

        $this->bus->dispatch(new Envelope(new DoSomethingCommand('p'), [new ReceivedStamp('app_commands_notifier')]));
    }
}
