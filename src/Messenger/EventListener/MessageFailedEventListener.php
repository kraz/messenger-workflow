<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Messenger\EventListener;

use Kraz\MessengerWorkflow\Messenger\Stamp\MessageIdStamp;
use Kraz\MessengerWorkflow\Messenger\Stamp\ResultStorageStamp;
use Kraz\MessengerWorkflow\Messenger\Stamp\TargetTransportNameStamp;
use Kraz\MessengerWorkflow\Messenger\Stamp\WillRetryMessageStamp;
use Kraz\MessengerWorkflow\Messenger\Transport\ResultStorageInterface;
use Symfony\Component\EventDispatcher\EventSubscriberInterface;
use Symfony\Component\Messenger\Event\WorkerMessageFailedEvent;
use Symfony\Component\Messenger\Stamp\ErrorDetailsStamp;

class MessageFailedEventListener implements EventSubscriberInterface
{
    public function __construct(
        protected ResultStorageInterface $resultStorage,
    ) {
    }

    public function writeErrorToResultStorage(WorkerMessageFailedEvent $event): void
    {
        if ($event->willRetry()) {
            return;
        }
        $envelope = $event->getEnvelope();
        $messageId = $envelope->last(MessageIdStamp::class)?->getMessageId();
        $useResultStorage = $messageId && null !== $envelope->last(ResultStorageStamp::class);
        if (!$useResultStorage) {
            return;
        }
        $errorDetailsStamp = $envelope->last(ErrorDetailsStamp::class);
        $message = $errorDetailsStamp?->getExceptionMessage() ?? 'Execution failed unexpectedly!';
        $code = $errorDetailsStamp?->getExceptionCode();
        $class = $errorDetailsStamp?->getExceptionClass();
        $trace = $errorDetailsStamp?->getFlattenException()?->getTraceAsString();
        $this->resultStorage->writeError($messageId, $message, $code, $class, $trace);
    }

    public function handleFailedMessageRetry(WorkerMessageFailedEvent $event): void
    {
        $envelope = $event->getEnvelope();
        if (!$envelope->last(TargetTransportNameStamp::class)) {
            return;
        }
        if ($event->willRetry()) {
            $event->addStamps(new WillRetryMessageStamp());
        }
    }

    public static function getSubscribedEvents(): array
    {
        return [
            WorkerMessageFailedEvent::class => [
                ['writeErrorToResultStorage', -100],
                ['handleFailedMessageRetry', 99],
            ],
        ];
    }
}
