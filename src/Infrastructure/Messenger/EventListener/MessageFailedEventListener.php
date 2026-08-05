<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Infrastructure\Messenger\EventListener;

use Kraz\MessengerWorkflow\Application\Task\ResultStorageInterface;
use Kraz\MessengerWorkflow\Infrastructure\Messenger\Stamp\MessageIdStamp;
use Kraz\MessengerWorkflow\Infrastructure\Messenger\Stamp\ResultTrackedStamp;
use Kraz\MessengerWorkflow\Infrastructure\Messenger\Stamp\WillRetryMessageStamp;
use Symfony\Component\EventDispatcher\EventSubscriberInterface;
use Symfony\Component\Messenger\Event\WorkerMessageFailedEvent;
use Symfony\Component\Messenger\Exception\HandlerFailedException;

/**
 * Marks failed messages that will be retried with a WillRetryMessageStamp, so the
 * workflow transports' reject() can distinguish "keep the row for the retry" from
 * "remove permanently". Runs after Symfony's retry listener (priority 100) has decided.
 *
 * Additionally publishes the failure to the result storage for TRACKED messages that
 * will not retry — the awaiting caller then receives a TaskFailedException.
 */
class MessageFailedEventListener implements EventSubscriberInterface
{
    public function __construct(
        private readonly ResultStorageInterface $resultStorage,
    ) {
    }

    public function handleFailedMessageRetry(WorkerMessageFailedEvent $event): void
    {
        if ($event->willRetry()) {
            $event->addStamps(new WillRetryMessageStamp());
        }
    }

    public function writeErrorToResultStorage(WorkerMessageFailedEvent $event): void
    {
        if ($event->willRetry()) {
            return;
        }

        $envelope = $event->getEnvelope();
        $messageId = $envelope->last(MessageIdStamp::class)?->getMessageId();
        if (null === $messageId || '' === $messageId || null === $envelope->last(ResultTrackedStamp::class)) {
            return;
        }

        $throwable = $event->getThrowable();
        if ($throwable instanceof HandlerFailedException) {
            $wrappedExceptions = $throwable->getWrappedExceptions();
            $throwable = array_shift($wrappedExceptions) ?? $throwable;
        }

        $this->resultStorage->writeError(
            $messageId,
            '' !== $throwable->getMessage() ? $throwable->getMessage() : 'Execution failed unexpectedly!',
            $throwable->getCode(),
            $throwable::class,
            $throwable->getTraceAsString(),
        );
    }

    public static function getSubscribedEvents(): array
    {
        return [
            WorkerMessageFailedEvent::class => [
                ['handleFailedMessageRetry', 99],
                ['writeErrorToResultStorage', -100],
            ],
        ];
    }
}
