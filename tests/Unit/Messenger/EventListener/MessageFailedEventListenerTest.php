<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Unit\Messenger\EventListener;

use Kraz\MessengerWorkflow\Application\Exception\TaskFailedException;
use Kraz\MessengerWorkflow\Messenger\EventListener\MessageFailedEventListener;
use Kraz\MessengerWorkflow\Messenger\Stamp\MessageIdStamp;
use Kraz\MessengerWorkflow\Messenger\Stamp\ResultStorageStamp;
use Kraz\MessengerWorkflow\Messenger\Stamp\TargetTransportNameStamp;
use Kraz\MessengerWorkflow\Messenger\Stamp\WillRetryMessageStamp;
use Kraz\MessengerWorkflow\Messenger\Transport\InMemoryResultStorage;
use Kraz\MessengerWorkflow\Tests\Fixture\Message\TestCommand;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Event\WorkerMessageFailedEvent;
use Symfony\Component\Messenger\Stamp\ErrorDetailsStamp;
use Symfony\Component\Messenger\Stamp\StampInterface;

final class MessageFailedEventListenerTest extends TestCase
{
    private InMemoryResultStorage $storage;
    private MessageFailedEventListener $listener;

    protected function setUp(): void
    {
        $this->storage = new InMemoryResultStorage();
        $this->listener = new MessageFailedEventListener($this->storage);
    }

    /**
     * @param StampInterface[] $stamps
     */
    private function failedEvent(array $stamps, bool $willRetry = false): WorkerMessageFailedEvent
    {
        $exception = new \DomainException('handler blew up', 42);
        $stamps[] = ErrorDetailsStamp::create($exception);
        $event = new WorkerMessageFailedEvent(new Envelope(new TestCommand(), $stamps), 'app_commands', $exception);
        if ($willRetry) {
            $event->setForRetry();
        }

        return $event;
    }

    public function testErrorIsWrittenToResultStorageForTrackedMessages(): void
    {
        $event = $this->failedEvent([new MessageIdStamp('task-1'), new ResultStorageStamp()]);

        $this->listener->writeErrorToResultStorage($event);

        try {
            $this->storage->await('task-1', 1);
            self::fail('Expected TaskFailedException');
        } catch (TaskFailedException $e) {
            self::assertSame('handler blew up', $e->getMessage());
            self::assertSame(42, $e->getCode());
            self::assertSame(\DomainException::class, $e->getTaskClass());
        }
    }

    public function testNothingIsWrittenWhenMessageWillBeRetried(): void
    {
        $event = $this->failedEvent([new MessageIdStamp('task-2'), new ResultStorageStamp()], willRetry: true);

        $this->listener->writeErrorToResultStorage($event);

        $this->expectException(\Kraz\MessengerWorkflow\Messenger\Transport\Exception\ResultStorageWaitTimeoutException::class);
        $this->storage->await('task-2', 1);
    }

    public function testNothingIsWrittenWithoutResultStorageStamp(): void
    {
        $event = $this->failedEvent([new MessageIdStamp('task-3')]);

        $this->listener->writeErrorToResultStorage($event);

        $this->expectException(\Kraz\MessengerWorkflow\Messenger\Transport\Exception\ResultStorageWaitTimeoutException::class);
        $this->storage->await('task-3', 1);
    }

    public function testWillRetryStampIsAddedForInboxMessagesOnly(): void
    {
        $withTarget = $this->failedEvent([new TargetTransportNameStamp('app_commands')], willRetry: true);
        $this->listener->handleFailedMessageRetry($withTarget);
        self::assertNotNull($withTarget->getEnvelope()->last(WillRetryMessageStamp::class));

        $withoutTarget = $this->failedEvent([], willRetry: true);
        $this->listener->handleFailedMessageRetry($withoutTarget);
        self::assertNull($withoutTarget->getEnvelope()->last(WillRetryMessageStamp::class));

        $noRetry = $this->failedEvent([new TargetTransportNameStamp('app_commands')]);
        $this->listener->handleFailedMessageRetry($noRetry);
        self::assertNull($noRetry->getEnvelope()->last(WillRetryMessageStamp::class));
    }
}
