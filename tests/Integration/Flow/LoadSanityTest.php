<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Integration\Flow;

use Contracts\Demo\Command\DoSomethingCommand;
use Contracts\Demo\Event\OrderedEvent;
use Doctrine\DBAL\Connection as DbalConnection;
use Jwage\PhpAmqpLibMessengerBundle\Transport\AmqpTransport;
use Kraz\MessengerWorkflow\Application\Messenger\CommandBusInterface;
use Kraz\MessengerWorkflow\Application\Task\ResultStorageInterface;
use Kraz\MessengerWorkflow\Domain\OutboxBusInterface;
use Kraz\MessengerWorkflow\Infrastructure\Messenger\Stamp\MessageIdStamp;
use Kraz\MessengerWorkflow\Tests\Fixture\Handler\DoSomethingCommandHandler;
use Kraz\MessengerWorkflow\Tests\Fixture\Handler\OrderedEventsHandler;
use Kraz\MessengerWorkflow\Tests\Support\Infra;
use Kraz\MessengerWorkflow\Tests\Support\PostgresDbal;
use Kraz\MessengerWorkflow\Tests\Support\WorkflowKernelTestCase;
use Kraz\MessengerWorkflow\Tests\TestKernel\RedisResultStorageKernel;
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
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;
use Symfony\Component\Messenger\Transport\TransportFactoryInterface;
use Symfony\Component\Messenger\Transport\TransportInterface;
use Symfony\Component\Messenger\Worker;
use Symfony\Component\Uid\Uuid;

/**
 * Spec (hardening): load sanity — a batch of tracked commands completes the
 * full flow with every result correct; an ordered event stream stays FIFO end-to-end;
 * repeated worker --limit cycles neither leak database connections nor grow memory
 * unboundedly.
 */
#[Group('postgres')]
#[Group('rabbitmq')]
#[Group('redis')]
#[RequiresPhpExtension('pdo_pgsql')]
final class LoadSanityTest extends WorkflowKernelTestCase
{
    private const string COMMANDS_OUTBOX = 'app_commands_outbox';
    private const string EVENTS_OUTBOX = 'app_outbox';
    private const string COMMANDS_INBOX = 'app_commands';
    private const string NOTIFIER = 'app_commands_notifier';

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

        $dbal = $this->dbal();
        foreach (['zz_commands_outbox', 'zz_commands_notifier', 'zz_commands_inbox', 'zz_commands_inbox_index', 'zz_commands_failures', 'zz_events_outbox', 'zz_events_inbox', 'zz_events_inbox_index', 'zz_events_failures'] as $table) {
            $dbal->executeStatement(\sprintf('TRUNCATE TABLE "%s"', $table));
        }
        $channel = $amqp->channel();
        foreach (['app_commands', 'app_events'] as $queue) {
            $channel->queue_purge($queue);
        }
        $channel->close();
        $amqp->close();
        $keys = $this->redis->keys('rs:fk:*');
        foreach (\is_array($keys) ? $keys : [] as $key) {
            $this->redis->del($key);
        }

        DoSomethingCommandHandler::reset();
        OrderedEventsHandler::reset();
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

    private function transport(string $name): TransportInterface
    {
        $transport = self::getContainer()->get('messenger.transport.'.$name);
        self::assertInstanceOf(TransportInterface::class, $transport);

        return $transport;
    }

    /**
     * @param array<string, TransportInterface> $receivers
     */
    private function runWorkerWith(array $receivers, string $busId, int $messageLimit, int $timeLimit = 30): void
    {
        $container = self::getContainer();
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
            foreach ($receivers as $receiver) {
                if ($receiver instanceof AmqpTransport) {
                    $receiver->getConnection()->close();
                }
            }
        }
    }

    /**
     * @param list<string> $transportNames
     */
    private function runWorker(array $transportNames, string $busId, int $messageLimit, int $timeLimit = 30): void
    {
        $receivers = [];
        foreach ($transportNames as $name) {
            $receivers[$name] = $this->transport($name);
        }

        $this->runWorkerWith($receivers, $busId, $messageLimit, $timeLimit);
    }

    private function runQueueReceiver(string $broker, string $exchangeType, string $queue, int $messageLimit): void
    {
        $container = self::getContainer();
        $factory = $container->get('messenger.transport_factory');
        self::assertInstanceOf(TransportFactoryInterface::class, $factory);
        $serializer = $container->get('messenger.default_serializer');
        self::assertInstanceOf(SerializerInterface::class, $serializer);

        $transport = $factory->createTransport(Infra::amqpDsn(), [
            'transport_name' => $broker,
            'auto_setup' => false,
            'exchange' => ['name' => $broker, 'type' => $exchangeType],
            'queues' => [$queue => []],
        ], $serializer);

        $this->runWorkerWith([$broker => $transport], 'inbox.bus', $messageLimit);
    }

    public function testBatchOfTrackedCommandsCompletesTheFullFlow(): void
    {
        $batchSize = 25;
        $taskIds = [];

        foreach (range(1, $batchSize) as $i) {
            $taskId = null;
            $this->commandBus()->dispatch(
                new Envelope(new DoSomethingCommand('batch-'.$i), [new TransportNamesStamp([self::COMMANDS_OUTBOX])]),
                $taskId,
            );
            self::assertNotNull($taskId);
            $taskIds['batch-'.$i] = $taskId;
        }

        $this->runWorker([self::COMMANDS_OUTBOX], 'relay.bus', $batchSize);
        $this->runQueueReceiver('commands', 'direct', 'app_commands', $batchSize);
        $this->runWorker([self::COMMANDS_INBOX], 'command.bus', $batchSize);
        $this->runWorker([self::NOTIFIER], 'notifier.bus', $batchSize);

        $resultStorage = self::getContainer()->get(ResultStorageInterface::class);
        self::assertInstanceOf(ResultStorageInterface::class, $resultStorage);
        foreach ($taskIds as $payload => $taskId) {
            $this->commandBus()->await($taskId, 5);
            self::assertSame('done:'.$payload, $resultStorage->await($taskId, 1));
            self::assertSame(1, DoSomethingCommandHandler::attempts($payload), 'Every command was handled exactly once');
        }
    }

    public function testOrderedEventStreamStaysFifoEndToEnd(): void
    {
        $streamSize = 60;
        $outboxBus = self::getContainer()->get(OutboxBusInterface::class);
        self::assertInstanceOf(OutboxBusInterface::class, $outboxBus);

        $expected = [];
        foreach (range(1, $streamSize) as $i) {
            $outboxBus->publish(new OrderedEvent('e'.$i));
            $expected[] = 'e'.$i;
        }

        $this->runWorker([self::EVENTS_OUTBOX], 'relay.bus', $streamSize);
        $this->runQueueReceiver('events', 'topic', 'app_events', $streamSize);
        $this->runWorker(['app_events'], 'event.bus', $streamSize);

        self::assertSame($expected, OrderedEventsHandler::$invocations, 'The whole stream arrived in publish order');
    }

    public function testRepeatedLimitCyclesLeakNeitherConnectionsNorMemory(): void
    {
        $inbox = $this->transport(self::COMMANDS_INBOX);
        foreach (range(1, 20) as $i) {
            $inbox->send(new Envelope(new DoSomethingCommand('cycle-'.$i), [
                new MessageIdStamp((string) Uuid::v7()),
            ]));
        }

        // Warm-up cycle so lazily created services/connections do not count as growth.
        $this->runWorker([self::COMMANDS_INBOX], 'command.bus', 2, 10);

        $observer = PostgresDbal::createConnection();
        $countConnections = static function () use ($observer): int {
            $count = $observer->fetchOne('SELECT COUNT(*) FROM pg_stat_activity WHERE datname = current_database() AND pid <> pg_backend_pid()');
            self::assertIsNumeric($count);

            return (int) $count;
        };
        $connectionsBefore = $countConnections();
        $memoryBefore = memory_get_usage();

        foreach (range(1, 9) as $cycle) {
            $this->runWorker([self::COMMANDS_INBOX], 'command.bus', 2, 10);
        }

        self::assertSame(20, array_sum(DoSomethingCommandHandler::$attempts), 'All 10 cycles x limit 2 consumed the whole batch');
        self::assertLessThanOrEqual($connectionsBefore, $countConnections(), 'Worker cycles must not accumulate database connections');
        self::assertLessThan(4 * 1024 * 1024, memory_get_usage() - $memoryBefore, 'Worker cycles must not grow memory unboundedly');
        $observer->close();
    }
}
