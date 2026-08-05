<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Integration\Flow;

use Contracts\Mini\Command\MiniCommand;
use Contracts\Mini\Event\MiniEvent;
use Contracts\Nano\Command\NanoCommand;
use Doctrine\DBAL\Connection as DbalConnection;
use Jwage\PhpAmqpLibMessengerBundle\Transport\AmqpTransport;
use Kraz\MessengerWorkflow\Application\Messenger\CommandBusInterface;
use Kraz\MessengerWorkflow\Application\Task\ResultStorageInterface;
use Kraz\MessengerWorkflow\Domain\OutboxBusInterface;
use Kraz\MessengerWorkflow\Tests\Fixture\Handler\MiniCommandHandler;
use Kraz\MessengerWorkflow\Tests\Fixture\Handler\MiniEventHandler;
use Kraz\MessengerWorkflow\Tests\Fixture\Handler\NanoCommandHandler;
use Kraz\MessengerWorkflow\Tests\Support\Infra;
use Kraz\MessengerWorkflow\Tests\Support\WorkflowKernelTestCase;
use Kraz\MessengerWorkflow\Tests\TestKernel\ReducedFlowKernel;
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
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;
use Symfony\Component\Messenger\Transport\TransportFactoryInterface;
use Symfony\Component\Messenger\Transport\TransportInterface;
use Symfony\Component\Messenger\Worker;

/**
 * Spec (flow-flexibility matrix): the reduced flows with NO inbox segment.
 *
 * - "no inbox": outbox → broker → direct queue consumption; the orm-mapped queue gets
 *   a plain middleware transaction around its handlers; the "<queue>_notifier"
 *   convention still resolves the notifier outbox (command variant) — event variant
 *   publishes through the outbox bus and consumes the topic queue directly.
 * - "minimal": no outbox, no inbox, no notifier, no mapping — dispatch straight to the
 *   broker, handler without transaction, tracked result written directly by the
 *   handler worker.
 *
 * The full flows and the "no outbox"/"no notifier (untracked)" rows live in
 * CommandFlowTest/EventFlowTest against the full topology.
 */
#[Group('postgres')]
#[Group('rabbitmq')]
#[Group('redis')]
#[RequiresPhpExtension('pdo_pgsql')]
final class ReducedFlowTest extends WorkflowKernelTestCase
{
    private const string COMMANDS_OUTBOX = 'mini_outbox';
    private const string EVENTS_OUTBOX = 'mini_events_outbox';
    private const string NOTIFIER = 'mini_commands_notifier';

    private \Redis $redis;

    protected static function getKernelClass(): string
    {
        return ReducedFlowKernel::class;
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
        foreach (['zz_mini_outbox', 'zz_mini_events_outbox', 'zz_mini_notifier'] as $table) {
            $dbal->executeStatement(\sprintf('TRUNCATE TABLE "%s"', $table));
        }
        $channel = $amqp->channel();
        foreach (['mini_commands', 'nano_commands', 'mini_events'] as $queue) {
            $channel->queue_purge($queue);
        }
        $channel->close();
        $amqp->close();
        foreach ($this->resultKeys() as $key) {
            $this->redis->del($key);
        }

        MiniCommandHandler::reset();
        NanoCommandHandler::reset();
        MiniEventHandler::reset();
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
     * @param array<string, TransportInterface> $receivers
     */
    private function runWorkerWith(array $receivers, string $busId, int $messageLimit, int $timeLimit = 10): void
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
    private function runWorker(array $transportNames, string $busId, int $messageLimit): void
    {
        $receivers = [];
        foreach ($transportNames as $name) {
            $receivers[$name] = $this->transport($name);
        }

        $this->runWorkerWith($receivers, $busId, $messageLimit);
    }

    /**
     * The collapsed no-inbox worker: "messenger:consume <broker> --queues=<queue>"
     * running handlers directly on the flow bus (own transport instance/connection,
     * closed afterwards — a dangling prefetch consumer would steal messages from a
     * later worker).
     */
    private function runQueueHandlerWorker(string $broker, string $exchangeType, string $queue, string $busId, int $messageLimit): void
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

        $this->runWorkerWith([$broker => $transport], $busId, $messageLimit);
    }

    public function testNoInboxCommandFlowRunsTheHandlerInAMappedTransactionAndKeepsTheNotifierSegment(): void
    {
        $taskId = null;
        $this->commandBus()->dispatch(new Envelope(new MiniCommand('tx'), [new TransportNamesStamp([self::COMMANDS_OUTBOX])]), $taskId);
        self::assertNotNull($taskId);
        self::assertSame(1, $this->messageCount(self::COMMANDS_OUTBOX), 'The command is parked in the outbox');

        $this->runWorker([self::COMMANDS_OUTBOX], 'relay.bus', 1);
        self::assertSame(0, $this->messageCount(self::COMMANDS_OUTBOX));

        // No receiver relay — the handler worker consumes the broker queue directly.
        $this->runQueueHandlerWorker('commands', 'direct', 'mini_commands', 'command.bus', 1);

        self::assertSame([true], MiniCommandHandler::$inTransaction, 'The orm-mapped queue got a plain middleware transaction (no-inbox mode)');
        self::assertSame(1, $this->messageCount(self::NOTIFIER), 'The "<queue>_notifier" convention still resolves without an inbox');
        self::assertSame([], $this->resultKeys(), 'The handler worker itself never writes when a notifier exists');

        $this->runWorker([self::NOTIFIER], 'notifier.bus', 1);

        $this->commandBus()->await($taskId, 5);
        self::assertSame('mini:tx', $this->resultStorage()->await($taskId, 1));
    }

    public function testNoInboxEventFlowConsumesTheTopicQueueDirectly(): void
    {
        $outboxBus = self::getContainer()->get(OutboxBusInterface::class);
        self::assertInstanceOf(OutboxBusInterface::class, $outboxBus);
        $outboxBus->publish(new MiniEvent('hello'));

        self::assertSame(1, $this->messageCount(self::EVENTS_OUTBOX), 'The event is parked in the outbox');

        $this->runWorker([self::EVENTS_OUTBOX], 'relay.bus', 1);
        $this->runQueueHandlerWorker('events', 'topic', 'mini_events', 'event.bus', 1);

        self::assertSame(['hello'], MiniEventHandler::$received);
        self::assertSame([false], MiniEventHandler::$inTransaction, 'Unmapped event queues run without a transaction');
    }

    public function testMinimalFlowTrackedCommandWritesTheResultDirectly(): void
    {
        $taskId = null;
        $this->commandBus()->dispatch(new NanoCommand('now'), $taskId);
        self::assertNotNull($taskId);

        self::assertSame(0, $this->messageCount(self::COMMANDS_OUTBOX), 'No outbox segment — the dispatch went straight to the broker');

        $this->runQueueHandlerWorker('commands', 'direct', 'nano_commands', 'command.bus', 1);

        self::assertSame([false], NanoCommandHandler::$inTransaction, 'No mapping, no inbox — no transaction');
        self::assertSame(0, $this->messageCount(self::NOTIFIER), 'The mini notifier belongs to another queue');

        $this->commandBus()->await($taskId, 5);
        self::assertSame('nano:now', $this->resultStorage()->await($taskId, 1), 'The tracked result was written directly from the handler worker');
    }

    public function testMinimalFlowUntrackedCommandLeavesNoArtifacts(): void
    {
        $this->commandBus()->dispatch(new NanoCommand('silent'));

        $this->runQueueHandlerWorker('commands', 'direct', 'nano_commands', 'command.bus', 1);

        self::assertSame([false], NanoCommandHandler::$inTransaction);
        self::assertSame([], $this->resultKeys(), 'Untracked commands never touch the result storage');
    }
}
