<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Integration\Flow;

use Contracts\Demo\Command\DoSomethingCommand;
use Contracts\Demo\Command\UnhandledCommand;
use Doctrine\DBAL\Connection as DbalConnection;
use Kraz\MessengerWorkflow\Application\Exception\PendingOutboxMessageException;
use Kraz\MessengerWorkflow\Application\Exception\TaskFailedException;
use Kraz\MessengerWorkflow\Application\Exception\TaskTimeOutException;
use Kraz\MessengerWorkflow\Application\Messenger\CommandBusInterface;
use Kraz\MessengerWorkflow\Application\Task\ResultStorageInterface;
use Kraz\MessengerWorkflow\Tests\Fixture\Handler\DoSomethingCommandHandler;
use Kraz\MessengerWorkflow\Tests\Support\Infra;
use Kraz\MessengerWorkflow\Tests\Support\WorkflowKernelTestCase;
use Kraz\MessengerWorkflow\Tests\TestKernel\RedisResultStorageKernel;
use Jwage\PhpAmqpLibMessengerBundle\Transport\AmqpTransport;
use PHPUnit\Framework\Attributes\Group;
use PHPUnit\Framework\Attributes\RequiresPhpExtension;
use Symfony\Bundle\FrameworkBundle\Console\Application;
use Symfony\Component\Console\Input\ArrayInput;
use Symfony\Component\Console\Output\BufferedOutput;
use Symfony\Component\EventDispatcher\EventDispatcherInterface;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\EventListener\StopWorkerOnMessageLimitListener;
use Symfony\Component\Messenger\EventListener\StopWorkerOnTimeLimitListener;
use Symfony\Component\Messenger\MessageBusInterface;
use Symfony\Component\Messenger\Stamp\TransportNamesStamp;
use Symfony\Component\Messenger\Transport\Receiver\MessageCountAwareInterface;
use Symfony\Component\Messenger\Transport\TransportInterface;
use Symfony\Component\Messenger\Worker;

/**
 * Spec: the FULL command flow end-to-end over real infrastructure —
 * dispatch → outbox → RabbitMQ → inbox → handler (in the inbox transaction) →
 * notifier outbox → Redis result storage → await — plus the retry/DLQ policy
 * per failure class and the await guards.
 *
 * Segment-hop failure semantics (relay reject keeps the row, receiver nack
 * redelivers, dedup drops duplicates) are covered by the outbox/inbox transport
 * suites; here the handler and notifier hops fail in every retry-policy class.
 */
#[Group('postgres')]
#[Group('rabbitmq')]
#[Group('redis')]
#[RequiresPhpExtension('pdo_pgsql')]
final class CommandFlowTest extends WorkflowKernelTestCase
{
    private const string OUTBOX = 'app_commands_outbox';
    private const string BROKER = 'commands';
    private const string INBOX = 'app_commands';
    private const string NOTIFIER = 'app_commands_notifier';
    private const string FAILURES = 'app_commands_failures';

    private \Redis $redis;

    protected static function getKernelClass(): string
    {
        return RedisResultStorageKernel::class;
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->redis = Infra::requireRedis();
        $amqp = Infra::requireAmqp();

        self::bootKernel();
        $kernel = self::$kernel;
        self::assertNotNull($kernel);
        $application = new Application($kernel);
        $application->setAutoExit(false);
        $output = new BufferedOutput();
        self::assertSame(0, $application->run(new ArrayInput(['command' => 'messenger:setup-transports', '--no-interaction' => true]), $output), $output->fetch());

        // Clean slate: workflow tables, broker queue, result keys, handler state.
        $dbal = $this->dbal();
        foreach (['zz_commands_outbox', 'zz_commands_notifier', 'zz_commands_inbox', 'zz_commands_inbox_index', 'zz_commands_failures'] as $table) {
            $dbal->executeStatement(\sprintf('TRUNCATE TABLE "%s"', $table));
        }
        $channel = $amqp->channel();
        $channel->queue_purge('app_commands');
        $channel->close();
        $amqp->close();
        foreach ($this->resultKeys() as $key) {
            $this->redis->del($key);
        }
        DoSomethingCommandHandler::reset();
    }

    private function dbal(): DbalConnection
    {
        $connection = self::getContainer()->get('doctrine.dbal.postgres_connection');
        self::assertInstanceOf(DbalConnection::class, $connection);

        return $connection;
    }

    private function commandBus(): CommandBusInterface
    {
        $bus = self::getContainer()->get(CommandBusInterface::class);
        self::assertInstanceOf(CommandBusInterface::class, $bus);

        return $bus;
    }

    private function resultStorage(): ResultStorageInterface
    {
        $storage = self::getContainer()->get(ResultStorageInterface::class);
        self::assertInstanceOf(ResultStorageInterface::class, $storage);

        return $storage;
    }

    /**
     * @return list<string>
     */
    private function resultKeys(): array
    {
        $keys = $this->redis->keys('rs:fk:*');

        return \is_array($keys) ? array_values(array_filter($keys, is_string(...))) : [];
    }

    private function transport(string $name): TransportInterface
    {
        $transport = self::getContainer()->get('messenger.transport.'.$name);
        self::assertInstanceOf(TransportInterface::class, $transport);

        return $transport;
    }

    private function messageCount(string $transportName): int
    {
        $transport = $this->transport($transportName);
        self::assertInstanceOf(MessageCountAwareInterface::class, $transport);

        return $transport->getMessageCount();
    }

    /**
     * @param list<string> $transportNames
     */
    private function runWorker(array $transportNames, string $busId, int $messageLimit, int $timeLimit = 10): void
    {
        $container = self::getContainer();
        $receivers = [];
        foreach ($transportNames as $name) {
            $receivers[$name] = $this->transport($name);
        }
        $bus = $container->get($busId);
        self::assertInstanceOf(MessageBusInterface::class, $bus);
        $dispatcher = $container->get('event_dispatcher');
        self::assertInstanceOf(EventDispatcherInterface::class, $dispatcher);

        $stopOnLimit = new StopWorkerOnMessageLimitListener($messageLimit);
        $stopOnTime = new StopWorkerOnTimeLimitListener($timeLimit);
        $dispatcher->addSubscriber($stopOnLimit);
        $dispatcher->addSubscriber($stopOnTime);
        try {
            new Worker($receivers, $bus, $dispatcher)->run(['sleep' => 50000]);
        } finally {
            $dispatcher->removeSubscriber($stopOnLimit);
            $dispatcher->removeSubscriber($stopOnTime);
            // A dangling consumer (open prefetch window) would steal messages
            // published by the NEXT test in this process.
            foreach ($receivers as $receiver) {
                if ($receiver instanceof AmqpTransport) {
                    $receiver->getConnection()->close();
                }
            }
        }
    }

    /**
     * Runs the publisher, receiver and handler workers; $deliveries > 1 lets the
     * handler worker consume retry redeliveries of the same message.
     */
    private function runFlowUntilHandled(int $deliveries = 1): void
    {
        $this->runWorker([self::OUTBOX], 'relay.bus', 1);
        $this->runWorker([self::BROKER], 'inbox.bus', 1);
        $this->runWorker([self::INBOX], 'command.bus', $deliveries);
    }

    private function dispatchThroughOutbox(object $command): string
    {
        $envelope = new Envelope($command, [new TransportNamesStamp([self::OUTBOX])]);
        $taskId = null;
        $this->commandBus()->dispatch($envelope, $taskId);
        self::assertNotNull($taskId);

        return $taskId;
    }

    public function testFullTrackedCommandFlow(): void
    {
        $taskId = $this->dispatchThroughOutbox(new DoSomethingCommand('e2e'));

        self::assertSame(1, $this->messageCount(self::OUTBOX), 'dispatch() stored the command in the outbox only');
        self::assertSame([], $this->resultKeys(), 'No result artifacts before the flow ran');

        $this->runWorker([self::OUTBOX], 'relay.bus', 1);
        self::assertSame(0, $this->messageCount(self::OUTBOX), 'Publisher worker relayed and acked the row');

        $this->runWorker([self::BROKER], 'inbox.bus', 1);
        self::assertSame(1, $this->messageCount(self::INBOX), 'Receiver worker moved the message into the inbox');

        $this->runWorker([self::INBOX], 'command.bus', 1);
        self::assertSame(0, $this->messageCount(self::INBOX), 'Handler worker consumed the inbox row transactionally');
        self::assertSame(1, $this->messageCount(self::NOTIFIER), 'The result notification is parked in the notifier outbox');
        self::assertSame([], $this->resultKeys(), 'The handler worker itself never writes to the result storage');

        $this->runWorker([self::NOTIFIER], 'notifier.bus', 1);
        self::assertSame(0, $this->messageCount(self::NOTIFIER));

        $this->commandBus()->await($taskId, 5);
        self::assertSame('done:e2e', $this->resultStorage()->await($taskId, 1), 'The handler result is readable from the result storage');
        self::assertSame(1, DoSomethingCommandHandler::attempts('e2e'));
        self::assertSame(0, $this->messageCount(self::FAILURES));
    }

    public function testNoOutboxVariantDispatchesStraightToTheBroker(): void
    {
        // Flow-matrix row "no outbox": without a TransportNamesStamp
        // override the marker-interface routing sends the command directly to the
        // AMQP transport — a dual write is impossible here because nothing else is
        // written, but broker downtime now surfaces at dispatch (informed trade-off).
        $taskId = null;
        $this->commandBus()->dispatch(new DoSomethingCommand('no-outbox'), $taskId);
        self::assertNotNull($taskId);

        self::assertSame(0, $this->messageCount(self::OUTBOX), 'The outbox was never touched');

        $this->runWorker([self::BROKER], 'inbox.bus', 1);
        $this->runWorker([self::INBOX], 'command.bus', 1);
        $this->runWorker([self::NOTIFIER], 'notifier.bus', 1);

        $this->commandBus()->await($taskId, 5);
        self::assertSame('done:no-outbox', $this->resultStorage()->await($taskId, 1));
        self::assertSame(1, DoSomethingCommandHandler::attempts('no-outbox'));
    }

    public function testUntrackedCommandsProduceNoResultArtifactsAnywhere(): void
    {
        $this->commandBus()->dispatch(new Envelope(new DoSomethingCommand('silent'), [new TransportNamesStamp([self::OUTBOX])]));

        $this->runFlowUntilHandled();

        self::assertSame(1, DoSomethingCommandHandler::attempts('silent'), 'The command was handled');
        self::assertSame(0, $this->messageCount(self::NOTIFIER), 'No notifier interaction for untracked commands');
        self::assertSame([], $this->resultKeys(), 'No result storage interaction for untracked commands');
    }

    public function testPermanentHandlerFailureSkipsRetriesAndReportsTheError(): void
    {
        $taskId = $this->dispatchThroughOutbox(new DoSomethingCommand('fail'));

        $this->runFlowUntilHandled();

        self::assertSame(1, DoSomethingCommandHandler::attempts('fail'), 'Commands are not retried by default');
        self::assertSame(1, $this->messageCount(self::FAILURES), 'The message went to the failure transport (DLQ)');
        self::assertSame(0, $this->messageCount(self::INBOX), 'At-most-once: the inbox row is removed even on failure');

        try {
            $this->commandBus()->await($taskId, 5);
            self::fail('The awaiting caller must receive the failure');
        } catch (TaskFailedException $exception) {
            self::assertSame('command failed permanently', $exception->getMessage());
            self::assertSame(42, $exception->getCode());
            self::assertSame(\RuntimeException::class, $exception->getTaskClass());
        }
    }

    public function testUnrecoverableFailuresAreNeverRetried(): void
    {
        $taskId = $this->dispatchThroughOutbox(new DoSomethingCommand('unrecoverable'));

        $this->runFlowUntilHandled();

        self::assertSame(1, DoSomethingCommandHandler::attempts('unrecoverable'));
        self::assertSame(1, $this->messageCount(self::FAILURES));

        $this->expectException(TaskFailedException::class);
        $this->commandBus()->await($taskId, 5);
    }

    public function testRecoverableFailuresAreRetriedUntilSuccess(): void
    {
        $taskId = $this->dispatchThroughOutbox(new DoSomethingCommand('recoverable-once'));

        $this->runFlowUntilHandled(deliveries: 2);
        $this->runWorker([self::NOTIFIER], 'notifier.bus', 1);

        self::assertSame(2, DoSomethingCommandHandler::attempts('recoverable-once'));
        self::assertSame(0, $this->messageCount(self::FAILURES));
        $this->commandBus()->await($taskId, 5);
        self::assertSame('recovered:2', $this->resultStorage()->await($taskId, 1));
    }

    public function testTransientInfrastructureFailuresAreRetried(): void
    {
        $taskId = $this->dispatchThroughOutbox(new DoSomethingCommand('transient-once'));

        $this->runFlowUntilHandled(deliveries: 2);
        $this->runWorker([self::NOTIFIER], 'notifier.bus', 1);

        self::assertSame(2, DoSomethingCommandHandler::attempts('transient-once'), 'The built-in transient decider forces a retry');
        self::assertSame(0, $this->messageCount(self::FAILURES));
        $this->commandBus()->await($taskId, 5);
        self::assertSame('transient-recovered:2', $this->resultStorage()->await($taskId, 1));
    }

    public function testApplicationRetryDecidersJoinTheDecisionChain(): void
    {
        $taskId = $this->dispatchThroughOutbox(new DoSomethingCommand('custom-retry-once'));

        $this->runFlowUntilHandled(deliveries: 2);
        $this->runWorker([self::NOTIFIER], 'notifier.bus', 1);

        self::assertSame(2, DoSomethingCommandHandler::attempts('custom-retry-once'), 'The tagged fixture decider forced the retry');
        $this->commandBus()->await($taskId, 5);
        self::assertSame('custom-recovered:2', $this->resultStorage()->await($taskId, 1));
    }

    public function testZeroHandlersIsAPermanentFailureReportedToTheAsker(): void
    {
        $taskId = $this->dispatchThroughOutbox(new UnhandledCommand('nobody'));

        $this->runFlowUntilHandled();

        self::assertSame(1, $this->messageCount(self::FAILURES));

        try {
            $this->commandBus()->await($taskId, 5);
            self::fail('The awaiting caller must receive the failure');
        } catch (TaskFailedException $exception) {
            self::assertStringContainsString('handled zero times', $exception->getMessage());
        }
    }

    public function testAwaitTimesOutWhenNoResultArrives(): void
    {
        $this->expectException(TaskTimeOutException::class);

        $this->commandBus()->await('01890000-0000-7000-8000-000000000000', 1);
    }

    public function testAwaitInsideTheDispatchingTransactionHitsTheDeadlockGuard(): void
    {
        $dbal = $this->dbal();

        $dbal->beginTransaction();
        try {
            $taskId = $this->dispatchThroughOutbox(new DoSomethingCommand('guarded'));

            try {
                $this->commandBus()->await($taskId, 2);
                self::fail('Awaiting inside the uncommitted dispatch transaction must throw');
            } catch (PendingOutboxMessageException $exception) {
                self::assertStringContainsString(self::OUTBOX, $exception->getMessage());
            }
        } finally {
            $dbal->rollBack();
        }

        self::assertSame(0, $this->messageCount(self::OUTBOX), 'The rollback also discarded the outbox row (outbox atomicity)');
    }
}
