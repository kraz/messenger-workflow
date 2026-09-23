<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Integration\Flow;

use Contracts\Demo\Command\DoSomethingCommand;
use Contracts\Demo\Command\PlanSomethingCommand;
use Contracts\Demo\Command\ReplanSomethingCommand;
use Doctrine\DBAL\Connection as DbalConnection;
use Jwage\PhpAmqpLibMessengerBundle\Transport\AmqpReceivedStamp;
use Jwage\PhpAmqpLibMessengerBundle\Transport\AmqpTransport;
use Kraz\MessengerWorkflow\Application\Exception\TaskFailedException;
use Kraz\MessengerWorkflow\Application\Messenger\CommandBusInterface;
use Kraz\MessengerWorkflow\Application\Task\ResultStorageInterface;
use Kraz\MessengerWorkflow\Tests\Fixture\Handler\DoSomethingCommandHandler;
use Kraz\MessengerWorkflow\Tests\Fixture\Handler\PlanSomethingCommandHandler;
use Kraz\MessengerWorkflow\Tests\Support\Infra;
use Kraz\MessengerWorkflow\Tests\Support\WorkflowKernelTestCase;
use Kraz\MessengerWorkflow\Tests\TestKernel\RoutedCommandsKernel;
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
use Symfony\Component\Messenger\Transport\Receiver\QueueReceiverInterface;
use Symfony\Component\Messenger\Transport\TransportInterface;
use Symfony\Component\Messenger\Worker;

/**
 * Spec: a routed command reaches ONLY its dedicated queue and inbox, exactly once —
 * dispatched straight to the broker or relayed from the command outbox — while the
 * context's other commands reach only the regular queue. On the single-consumer routed
 * inbox, commands are handled in dispatch order; tracked routed commands publish their
 * result through the context's notifier; failures land in the routed queue's DLQ.
 */
#[Group('postgres')]
#[Group('rabbitmq')]
#[Group('redis')]
#[RequiresPhpExtension('pdo_pgsql')]
final class RoutedCommandFlowTest extends WorkflowKernelTestCase
{
    private const string OUTBOX = 'app_commands_outbox';
    private const string BROKER = 'commands';
    private const string REGULAR_INBOX = 'app_commands';
    private const string ROUTED_INBOX = 'app_planning';
    private const string NOTIFIER = 'app_commands_notifier';

    private \Redis $redis;

    protected static function getKernelClass(): string
    {
        return RoutedCommandsKernel::class;
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

        $dbal = $this->dbal();
        foreach ([
            'zz_commands_outbox', 'zz_commands_notifier', 'zz_commands_failures',
            'zz_commands_inbox', 'zz_commands_inbox_index',
            'zz_commands_inbox_app_planning', 'zz_commands_inbox_app_planning_index',
        ] as $table) {
            $dbal->executeStatement(\sprintf('TRUNCATE TABLE "%s"', $table));
        }
        $channel = $amqp->channel();
        $channel->queue_purge('app_commands');
        $channel->queue_purge('app_planning');
        $channel->close();
        $amqp->close();
        foreach ($this->resultKeys() as $key) {
            $this->redis->del($key);
        }
        DoSomethingCommandHandler::reset();
        PlanSomethingCommandHandler::reset();
    }

    protected function tearDown(): void
    {
        if (null !== self::$kernel) {
            $transport = self::getContainer()->get('messenger.transport.'.self::BROKER);
            if ($transport instanceof AmqpTransport) {
                $transport->getConnection()->close();
            }
        }

        parent::tearDown();
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
     * The jwage transport runs ONE consumer per connection, so a broker worker
     * consumes one queue — exactly the shape of the derived receiver workers
     * ("messenger:consume --queues=<queue> commands"); $queues selects it.
     *
     * @param list<string> $transportNames
     * @param list<string> $queues
     */
    private function runWorker(array $transportNames, string $busId, int $messageLimit, int $timeLimit = 10, array $queues = []): void
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
            new Worker($receivers, $bus, $dispatcher)->run(['sleep' => 50000] + ([] !== $queues ? ['queues' => $queues] : []));
        } finally {
            $dispatcher->removeSubscriber($stopOnLimit);
            $dispatcher->removeSubscriber($stopOnTime);
            foreach ($receivers as $receiver) {
                if ($receiver instanceof AmqpTransport) {
                    $receiver->getConnection()->close();
                }
            }
        }
    }

    /**
     * Drains one broker queue: the messages with their routing keys, acked.
     *
     * @return list<array{class: class-string, routing_key: string|null}>
     */
    private function drainQueue(string $queueName, float $timeoutSeconds = 3.0): array
    {
        $transport = $this->transport(self::BROKER);
        self::assertInstanceOf(QueueReceiverInterface::class, $transport);

        $drained = [];
        $deadline = microtime(true) + $timeoutSeconds;
        $idleRounds = 0;
        while (microtime(true) < $deadline && $idleRounds < 2) {
            $received = false;
            foreach ($transport->getFromQueues([$queueName]) as $envelope) {
                $drained[] = [
                    'class' => $envelope->getMessage()::class,
                    'routing_key' => $envelope->last(AmqpReceivedStamp::class)?->getAmqpEnvelope()->getRoutingKey(),
                ];
                $transport->ack($envelope);
                $received = true;
            }
            $idleRounds = $received ? 0 : $idleRounds + 1;
        }
        // Release the queue's consumer so the next drain can subscribe to another queue.
        self::assertInstanceOf(AmqpTransport::class, $transport);
        $transport->getConnection()->close();

        return $drained;
    }

    public function testRoutedCommandsReachOnlyTheRoutedQueueAndUnroutedOnesOnlyTheRegularQueue(): void
    {
        $this->commandBus()->dispatch(new PlanSomethingCommand('listed'));
        $this->commandBus()->dispatch(new ReplanSomethingCommand('attributed'));
        $this->commandBus()->dispatch(new DoSomethingCommand('regular'));

        self::assertSame([
            ['class' => PlanSomethingCommand::class, 'routing_key' => 'commands.Demo.planning'],
            ['class' => ReplanSomethingCommand::class, 'routing_key' => 'commands.Demo.planning'],
        ], $this->drainQueue('app_planning'));
        self::assertSame([
            ['class' => DoSomethingCommand::class, 'routing_key' => 'commands.Demo'],
        ], $this->drainQueue('app_commands'));
    }

    public function testTheOutboxRelayRoutesExactlyLikeADirectDispatch(): void
    {
        foreach ([new PlanSomethingCommand('listed'), new ReplanSomethingCommand('attributed'), new DoSomethingCommand('regular')] as $command) {
            $this->commandBus()->dispatch(new Envelope($command, [new TransportNamesStamp([self::OUTBOX])]));
        }
        self::assertSame(3, $this->messageCount(self::OUTBOX));

        $this->runWorker([self::OUTBOX], 'relay.bus', 3);
        self::assertSame(0, $this->messageCount(self::OUTBOX));

        // One receiver worker per queue (as derived), each relaying its deliveries
        // into the inbox named after the queue.
        $this->runWorker([self::BROKER], 'inbox.bus', 2, queues: [self::ROUTED_INBOX]);
        $this->runWorker([self::BROKER], 'inbox.bus', 1, queues: [self::REGULAR_INBOX]);

        self::assertSame(2, $this->messageCount(self::ROUTED_INBOX), 'Both routed commands, once each, in the routed inbox');
        self::assertSame(1, $this->messageCount(self::REGULAR_INBOX), 'The regular command, once, in the regular inbox');
    }

    public function testATrackedRoutedCommandPublishesItsResultThroughTheContextsNotifier(): void
    {
        $taskId = null;
        $this->commandBus()->dispatch(new Envelope(new PlanSomethingCommand('tracked'), [new TransportNamesStamp([self::OUTBOX])]), $taskId);
        self::assertNotNull($taskId);

        $this->runWorker([self::OUTBOX], 'relay.bus', 1);
        $this->runWorker([self::BROKER], 'inbox.bus', 1, queues: [self::ROUTED_INBOX]);
        self::assertSame(1, $this->messageCount(self::ROUTED_INBOX));

        $this->runWorker([self::ROUTED_INBOX], 'command.bus', 1);
        self::assertSame(['tracked'], PlanSomethingCommandHandler::$handled);
        self::assertSame(0, $this->messageCount(self::ROUTED_INBOX), 'Consumed transactionally');
        self::assertSame(1, $this->messageCount(self::NOTIFIER), 'One notification row in the CONTEXT notifier outbox');
        self::assertSame([], $this->resultKeys(), 'No direct write to the result storage (no reduced-flow fallback)');

        $this->runWorker([self::NOTIFIER], 'notifier.bus', 1);
        $this->commandBus()->await($taskId, 5);
        self::assertSame('planned:tracked', $this->resultStorage()->await($taskId, 1));
    }

    public function testCommandsOnTheRoutedInboxAreHandledInDispatchOrder(): void
    {
        $payloads = ['o1', 'o2', 'o3', 'o4', 'o5', 'o6'];
        foreach ($payloads as $payload) {
            $this->commandBus()->dispatch(new Envelope(new PlanSomethingCommand($payload), [new TransportNamesStamp([self::OUTBOX])]));
        }

        $this->runWorker([self::OUTBOX], 'relay.bus', \count($payloads));
        $this->runWorker([self::BROKER], 'inbox.bus', \count($payloads), queues: [self::ROUTED_INBOX]);
        $this->runWorker([self::ROUTED_INBOX], 'command.bus', \count($payloads));

        self::assertSame($payloads, PlanSomethingCommandHandler::$handled);
    }

    public function testAFailingRoutedCommandLandsInTheRoutedQueuesFailureTransport(): void
    {
        $taskId = null;
        $this->commandBus()->dispatch(new PlanSomethingCommand('fail-now'), $taskId);
        self::assertNotNull($taskId);

        $this->runWorker([self::BROKER], 'inbox.bus', 1, queues: [self::ROUTED_INBOX]);
        $this->runWorker([self::ROUTED_INBOX], 'command.bus', 1);

        self::assertSame(1, $this->messageCount('app_planning_failures'));
        self::assertSame(0, $this->messageCount('app_commands_failures'), 'The shared failures table is filtered by queue_name');
        self::assertSame(0, $this->messageCount(self::ROUTED_INBOX));

        $this->expectException(TaskFailedException::class);
        $this->commandBus()->await($taskId, 5);
    }
}
