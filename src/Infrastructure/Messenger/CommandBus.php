<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Infrastructure\Messenger;

use Kraz\MessengerWorkflow\Application\CommandInterface;
use Kraz\MessengerWorkflow\Application\Exception\PendingOutboxMessageException;
use Kraz\MessengerWorkflow\Application\Exception\TaskTimeOutException;
use Kraz\MessengerWorkflow\Application\Messenger\CommandBusInterface;
use Kraz\MessengerWorkflow\Application\Task\Exception\ResultStorageWaitTimeoutException;
use Kraz\MessengerWorkflow\Application\Task\ResultStorageInterface;
use Kraz\MessengerWorkflow\Infrastructure\Doctrine\WorkflowTransportRegistry;
use Kraz\MessengerWorkflow\Infrastructure\Messenger\Stamp\MessageIdStamp;
use Kraz\MessengerWorkflow\Infrastructure\Messenger\Stamp\ResultTrackedStamp;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\MessageBusInterface;
use Symfony\Component\Uid\Uuid;

/**
 * Command bus: always-async dispatch through the broker (or the configured
 * outbox — pass a TransportNamesStamp-wrapped Envelope to override the routed
 * senders). Result tracking is opt-in via the by-reference $taskId argument:
 * argument presence — not a non-null value — activates it.
 */
class CommandBus implements CommandBusInterface
{
    public function __construct(
        private readonly MessageBusInterface $messageBus,
        private readonly ResultStorageInterface $resultStorage,
        private readonly WorkflowTransportRegistry $transportRegistry,
        private readonly int $awaitDefaultTimeout = 300,
    ) {
    }

    public function dispatch(object $command, ?string &$taskId = null): void
    {
        $envelope = Envelope::wrap($command);
        if (!$envelope->getMessage() instanceof CommandInterface) {
            throw new \RuntimeException(\sprintf('Invalid command message. Expected an instance of "%s", but got %s', CommandInterface::class, get_debug_type($envelope->getMessage())));
        }

        $messageId = $envelope->last(MessageIdStamp::class)?->getMessageId() ?? (string) Uuid::v7();
        $envelope = $envelope
            ->withoutAll(MessageIdStamp::class)
            ->with(new MessageIdStamp($messageId));

        if (\func_num_args() >= 2) {
            $envelope = $envelope
                ->withoutAll(ResultTrackedStamp::class)
                ->with(new ResultTrackedStamp());
            $taskId = $messageId;
        }

        $this->messageBus->dispatch($envelope);
    }

    public function await(string $taskId, ?int $timeout = null): void
    {
        $this->assertNotPendingInUncommittedOutbox($taskId);

        try {
            $this->resultStorage->await($taskId, $timeout ?? $this->awaitDefaultTimeout);
        } catch (ResultStorageWaitTimeoutException $exception) {
            throw new TaskTimeOutException(\sprintf('Command task "%s" did not complete within the timeout.', $taskId), 0, $exception);
        }
    }

    /**
     * Deadlock guard: a task whose message still sits in an outbox on a database
     * connection with an active transaction of THIS process cannot be relayed until that
     * transaction commits — awaiting it would only ever time out.
     */
    private function assertNotPendingInUncommittedOutbox(string $taskId): void
    {
        foreach ($this->transportRegistry->getOutboxTransports() as $transportName => $transport) {
            $connection = $transport->getConnection();
            if (!$connection->getDriverConnection()->isTransactionActive()) {
                continue;
            }

            if ($connection->hasMessageContaining($taskId)) {
                throw new PendingOutboxMessageException(\sprintf('Task "%s" is still stored in outbox transport "%s" within an active (uncommitted) database transaction of this process. The message cannot be relayed — and await() would time out — until that transaction commits.', $taskId, $transportName));
            }
        }
    }
}
