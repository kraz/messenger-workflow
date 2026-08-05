<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Unit\Infrastructure\Messenger;

use Contracts\Demo\Command\DoSomethingCommand;
use Doctrine\DBAL\DriverManager;
use Kraz\MessengerWorkflow\Application\Exception\PendingOutboxMessageException;
use Kraz\MessengerWorkflow\Application\Exception\TaskFailedException;
use Kraz\MessengerWorkflow\Application\Exception\TaskTimeOutException;
use Kraz\MessengerWorkflow\Application\Messenger\CommandBusInterface;
use Kraz\MessengerWorkflow\Infrastructure\Doctrine\Connection;
use Kraz\MessengerWorkflow\Infrastructure\Doctrine\Outbox\OutboxTransport;
use Kraz\MessengerWorkflow\Infrastructure\Doctrine\WorkflowTransportRegistry;
use Kraz\MessengerWorkflow\Infrastructure\Messenger\CommandBus;
use Kraz\MessengerWorkflow\Infrastructure\Messenger\Stamp\MessageIdStamp;
use Kraz\MessengerWorkflow\Infrastructure\Messenger\Stamp\ResultTrackedStamp;
use Kraz\MessengerWorkflow\Infrastructure\Task\InMemoryResultStorage;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\MessageBusInterface;
use Symfony\Component\Messenger\Stamp\TransportNamesStamp;
use Symfony\Component\Messenger\Transport\Serialization\PhpSerializer;
use Symfony\Component\Uid\Uuid;

final class CommandBusTest extends TestCase
{
    /**
     * @var MessageBusInterface&object{envelopes: list<Envelope>}
     */
    private MessageBusInterface $messageBus;
    private InMemoryResultStorage $resultStorage;
    private WorkflowTransportRegistry $transportRegistry;
    private CommandBus $commandBus;

    protected function setUp(): void
    {
        $this->messageBus = new class implements MessageBusInterface {
            /**
             * @var list<Envelope>
             */
            public array $envelopes = [];

            public function dispatch(object $message, array $stamps = []): Envelope
            {
                return $this->envelopes[] = Envelope::wrap($message, $stamps);
            }
        };
        $this->resultStorage = new InMemoryResultStorage();
        $this->transportRegistry = new WorkflowTransportRegistry();
        $this->commandBus = new CommandBus($this->messageBus, $this->resultStorage, $this->transportRegistry);
    }

    public function testDispatchRejectsNonCommandMessages(): void
    {
        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessageMatches('/Invalid command message/');

        $this->commandBus->dispatch(new \stdClass());
    }

    public function testUntrackedDispatchIsFireAndForgetWithoutResultTracking(): void
    {
        $this->commandBus->dispatch(new DoSomethingCommand('payload'));

        self::assertCount(1, $this->messageBus->envelopes);
        $envelope = $this->messageBus->envelopes[0];
        self::assertNotNull($envelope->last(MessageIdStamp::class));
        self::assertNull($envelope->last(ResultTrackedStamp::class), 'Untracked dispatch must not mark the message result-tracked');
    }

    public function testTrackedDispatchReturnsTheTaskIdByReferenceAndStampsTheEnvelope(): void
    {
        $taskId = null;
        $this->commandBus->dispatch(new DoSomethingCommand('payload'), $taskId);

        $envelope = $this->messageBus->envelopes[0];
        $messageIdStamp = $envelope->last(MessageIdStamp::class);
        self::assertNotNull($messageIdStamp);
        self::assertSame($messageIdStamp->getMessageId(), $taskId);
        self::assertNotNull($envelope->last(ResultTrackedStamp::class));
        self::assertTrue(Uuid::isValid($taskId));
    }

    public function testTrackingIsActivatedByArgumentPresenceNotByValue(): void
    {
        // D1: an explicitly passed variable — even one already null — activates tracking.
        $taskId = null;
        $this->commandBus->dispatch(new DoSomethingCommand('payload'), $taskId);
        self::assertNotNull($taskId);
    }

    public function testDispatchAcceptsAPreWrappedEnvelopeAndKeepsItsStamps(): void
    {
        $existingId = (string) Uuid::v7();
        $envelope = new Envelope(new DoSomethingCommand('payload'), [
            new MessageIdStamp($existingId),
            new TransportNamesStamp(['app_commands_outbox']),
        ]);

        $taskId = null;
        $this->commandBus->dispatch($envelope, $taskId);

        self::assertSame($existingId, $taskId, 'A pre-stamped message id is reused as the Task ID');
        $dispatched = $this->messageBus->envelopes[0];
        self::assertCount(1, $dispatched->all(MessageIdStamp::class));
        self::assertNotNull($dispatched->last(TransportNamesStamp::class), 'Transport override stamps travel with the dispatch');
    }

    public function testAwaitTimeoutIsTranslatedToTaskTimeOutException(): void
    {
        $this->expectException(TaskTimeOutException::class);

        $this->commandBus->await('01890000-0000-7000-8000-000000000000', 1);
    }

    public function testAwaitPropagatesTaskFailures(): void
    {
        $this->resultStorage->writeError('task-1', 'boom', 7, \RuntimeException::class, 'trace');

        try {
            $this->commandBus->await('task-1');
            self::fail('A failed task must throw');
        } catch (TaskFailedException $exception) {
            self::assertSame('boom', $exception->getMessage());
            self::assertSame(\RuntimeException::class, $exception->getTaskClass());
            self::assertSame('trace', $exception->getTaskTrace());
        }
    }

    public function testAwaitReturnsVoidOnSuccess(): void
    {
        $this->resultStorage->write('task-2', ['some' => 'value']);

        $this->commandBus->await('task-2');
        $this->addToAssertionCount(1);
    }

    public function testAwaitThrowsWhenTheTaskIsParkedInAnUncommittedOutbox(): void
    {
        $dbal = DriverManager::getConnection(['driver' => 'pdo_sqlite', 'memory' => true]);
        $transport = new OutboxTransport(new Connection(['table_name' => 'outbox', 'index_table_name' => 'outbox_idx'], $dbal), new PhpSerializer());
        $transport->setup();
        $this->transportRegistry->addOutboxTransport('app_commands_outbox', $transport);

        $dbal->beginTransaction();
        try {
            $transport->send(new Envelope(new DoSomethingCommand('payload'), [new MessageIdStamp((string) Uuid::v7())]));
            $sentTaskId = $this->latestMessageId($transport);

            try {
                $this->commandBus->await($sentTaskId, 1);
                self::fail('Awaiting a task held by the uncommitted outbox must throw');
            } catch (PendingOutboxMessageException $exception) {
                self::assertStringContainsString('app_commands_outbox', $exception->getMessage());
            }
        } finally {
            $dbal->commit();
        }

        // After the commit the guard no longer applies — the await times out normally
        // (nothing relays the outbox in this test).
        $this->expectException(TaskTimeOutException::class);
        $this->commandBus->await($sentTaskId, 1);
    }

    public function testAwaitIgnoresOutboxesWithoutAnActiveTransaction(): void
    {
        $dbal = DriverManager::getConnection(['driver' => 'pdo_sqlite', 'memory' => true]);
        $transport = new OutboxTransport(new Connection(['table_name' => 'outbox', 'index_table_name' => 'outbox_idx'], $dbal), new PhpSerializer());
        $transport->setup();
        $this->transportRegistry->addOutboxTransport('app_commands_outbox', $transport);

        $transport->send(new Envelope(new DoSomethingCommand('payload'), [new MessageIdStamp((string) Uuid::v7())]));

        $this->expectException(TaskTimeOutException::class);
        $this->commandBus->await($this->latestMessageId($transport), 1);
    }

    /**
     * Decorators must forward the ARGUMENT PRESENCE, not just the value — the pattern
     * used by the TrackingCommandBus decorator.
     */
    public function testDecoratorsCanForwardArgumentPresence(): void
    {
        $decorator = new class($this->commandBus) implements CommandBusInterface {
            public function __construct(private readonly CommandBusInterface $inner)
            {
            }

            public function dispatch(object $command, ?string &$taskId = null): void
            {
                if (\func_num_args() >= 2) {
                    $this->inner->dispatch($command, $taskId);

                    return;
                }

                $this->inner->dispatch($command);
            }

            public function await(string $taskId, ?int $timeout = null): void
            {
                $this->inner->await($taskId, $timeout);
            }
        };

        $decorator->dispatch(new DoSomethingCommand('untracked'));
        self::assertNull($this->messageBus->envelopes[0]->last(ResultTrackedStamp::class));

        $taskId = null;
        $decorator->dispatch(new DoSomethingCommand('tracked'), $taskId);
        self::assertNotNull($taskId);
        self::assertNotNull($this->messageBus->envelopes[1]->last(ResultTrackedStamp::class));
    }

    private function latestMessageId(OutboxTransport $transport): string
    {
        $envelopes = [...$transport->all()];
        $last = end($envelopes);
        self::assertInstanceOf(Envelope::class, $last);
        $messageId = $last->last(MessageIdStamp::class)?->getMessageId();
        self::assertNotNull($messageId);

        return $messageId;
    }
}
