<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Infrastructure\Messenger;

use Kraz\MessengerWorkflow\Application\Exception\TaskTimeOutException;
use Kraz\MessengerWorkflow\Application\Messenger\QueryBusInterface;
use Kraz\MessengerWorkflow\Application\QueryInterface;
use Kraz\MessengerWorkflow\Application\Task\Exception\ResultStorageWaitTimeoutException;
use Kraz\MessengerWorkflow\Application\Task\ResultStorageInterface;
use Kraz\MessengerWorkflow\Infrastructure\Messenger\Stamp\MessageIdStamp;
use Kraz\MessengerWorkflow\Infrastructure\Messenger\Stamp\ResultTrackedStamp;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\MessageBusInterface;
use Symfony\Component\Uid\Uuid;

/**
 * Query bus (public API unchanged since 0.2): ask() = askAsync() + await(). Queries
 * always travel through the broker and always use the result storage — it is their
 * result channel; the handler worker writes the value (QueryResultMiddleware), the asker
 * blocks on await().
 */
class QueryBus implements QueryBusInterface
{
    public function __construct(
        private readonly MessageBusInterface $messageBus,
        private readonly ResultStorageInterface $resultStorage,
        private readonly int $awaitDefaultTimeout = 300,
    ) {
    }

    public function ask(object $query, ?int $timeout = null): mixed
    {
        return $this->await($this->askAsync($query), $timeout);
    }

    public function askAsync(object $query): string
    {
        $envelope = Envelope::wrap($query);
        if (!$envelope->getMessage() instanceof QueryInterface) {
            throw new \RuntimeException(\sprintf('Invalid query message. Expected an instance of "%s", but got %s', QueryInterface::class, get_debug_type($envelope->getMessage())));
        }

        $messageId = $envelope->last(MessageIdStamp::class)?->getMessageId() ?? (string) Uuid::v7();
        $envelope = $envelope
            ->withoutAll(MessageIdStamp::class)
            ->with(new MessageIdStamp($messageId))
            ->withoutAll(ResultTrackedStamp::class)
            ->with(new ResultTrackedStamp());

        $this->messageBus->dispatch($envelope);

        return $messageId;
    }

    public function await(string $taskId, ?int $timeout = null): mixed
    {
        try {
            return $this->resultStorage->await($taskId, $timeout ?? $this->awaitDefaultTimeout);
        } catch (ResultStorageWaitTimeoutException $exception) {
            throw new TaskTimeOutException(\sprintf('Query task "%s" did not complete within the timeout.', $taskId), 0, $exception);
        }
    }
}
