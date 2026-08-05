<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Unit\Infrastructure\Messenger\EventListener;

use Contracts\Demo\Command\DoSomethingCommand;
use Kraz\MessengerWorkflow\Application\Exception\TaskFailedException;
use Kraz\MessengerWorkflow\Application\Task\Exception\ResultStorageWaitTimeoutException;
use Kraz\MessengerWorkflow\Infrastructure\Messenger\EventListener\MessageFailedEventListener;
use Kraz\MessengerWorkflow\Infrastructure\Messenger\Stamp\MessageIdStamp;
use Kraz\MessengerWorkflow\Infrastructure\Messenger\Stamp\ResultTrackedStamp;
use Kraz\MessengerWorkflow\Infrastructure\Messenger\Stamp\WillRetryMessageStamp;
use Kraz\MessengerWorkflow\Infrastructure\Task\InMemoryResultStorage;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Event\WorkerMessageFailedEvent;
use Symfony\Component\Messenger\Exception\HandlerFailedException;
use Symfony\Component\Messenger\Stamp\StampInterface;

final class MessageFailedEventListenerTest extends TestCase
{
    private InMemoryResultStorage $resultStorage;
    private MessageFailedEventListener $listener;

    protected function setUp(): void
    {
        $this->resultStorage = new InMemoryResultStorage();
        $this->listener = new MessageFailedEventListener($this->resultStorage);
    }

    /**
     * @param list<StampInterface> $stamps
     */
    private function failedEvent(array $stamps, \Throwable $throwable, bool $willRetry = false): WorkerMessageFailedEvent
    {
        $event = new WorkerMessageFailedEvent(new Envelope(new DoSomethingCommand('p'), $stamps), 'app_commands', $throwable);
        if ($willRetry) {
            $event->setForRetry();
        }

        return $event;
    }

    public function testRetryingMessagesAreMarkedWithTheWillRetryStamp(): void
    {
        $event = $this->failedEvent([], new \RuntimeException(), willRetry: true);
        $this->listener->handleFailedMessageRetry($event);

        self::assertNotNull($event->getEnvelope()->last(WillRetryMessageStamp::class));
    }

    public function testPermanentlyFailedMessagesGetNoRetryStamp(): void
    {
        $event = $this->failedEvent([], new \RuntimeException());
        $this->listener->handleFailedMessageRetry($event);

        self::assertNull($event->getEnvelope()->last(WillRetryMessageStamp::class));
    }

    public function testAPermanentFailureOfATrackedMessageIsPublishedToTheResultStorage(): void
    {
        $event = $this->failedEvent(
            [new MessageIdStamp('task-1'), new ResultTrackedStamp()],
            new \RuntimeException('handler exploded', 13),
        );
        $this->listener->writeErrorToResultStorage($event);

        try {
            $this->resultStorage->await('task-1', 1);
            self::fail('The stored outcome must be a failure');
        } catch (TaskFailedException $exception) {
            self::assertSame('handler exploded', $exception->getMessage());
            self::assertSame(13, $exception->getCode());
            self::assertSame(\RuntimeException::class, $exception->getTaskClass());
            self::assertNotNull($exception->getTaskTrace());
        }
    }

    public function testHandlerFailedExceptionsAreUnwrappedToTheRealCause(): void
    {
        $envelope = new Envelope(new DoSomethingCommand('p'));
        $event = $this->failedEvent(
            [new MessageIdStamp('task-1'), new ResultTrackedStamp()],
            new HandlerFailedException($envelope, [new \DomainException('the real cause')]),
        );
        $this->listener->writeErrorToResultStorage($event);

        try {
            $this->resultStorage->await('task-1', 1);
            self::fail('The stored outcome must be a failure');
        } catch (TaskFailedException $exception) {
            self::assertSame('the real cause', $exception->getMessage());
            self::assertSame(\DomainException::class, $exception->getTaskClass());
        }
    }

    public function testRetryingFailuresAreNotPublished(): void
    {
        $event = $this->failedEvent(
            [new MessageIdStamp('task-1'), new ResultTrackedStamp()],
            new \RuntimeException('will be retried'),
            willRetry: true,
        );
        $this->listener->writeErrorToResultStorage($event);

        $this->expectException(ResultStorageWaitTimeoutException::class);
        $this->resultStorage->await('task-1', 1);
    }

    public function testUntrackedFailuresAreNotPublished(): void
    {
        $event = $this->failedEvent([new MessageIdStamp('task-1')], new \RuntimeException());
        $this->listener->writeErrorToResultStorage($event);

        $this->expectException(ResultStorageWaitTimeoutException::class);
        $this->resultStorage->await('task-1', 1);
    }

    public function testTrackedFailuresWithoutAMessageIdAreIgnored(): void
    {
        $event = $this->failedEvent([new ResultTrackedStamp()], new \RuntimeException());
        $this->listener->writeErrorToResultStorage($event);

        $this->addToAssertionCount(1);
    }
}
