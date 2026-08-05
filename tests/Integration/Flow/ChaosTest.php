<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Integration\Flow;

use Contracts\Chaos\Command\ChaosCommand;
use Doctrine\DBAL\Connection as DbalConnection;
use Jwage\PhpAmqpLibMessengerBundle\Transport\AmqpTransport;
use Kraz\MessengerWorkflow\Application\Messenger\CommandBusInterface;
use Kraz\MessengerWorkflow\Infrastructure\Messenger\Stamp\MessageIdStamp;
use Kraz\MessengerWorkflow\Tests\Fixture\Handler\ChaosCommandHandler;
use Kraz\MessengerWorkflow\Tests\Support\Infra;
use Kraz\MessengerWorkflow\Tests\Support\PostgresDbal;
use Kraz\MessengerWorkflow\Tests\Support\WorkflowKernelTestCase;
use Kraz\MessengerWorkflow\Tests\TestKernel\ChaosKernel;
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
use Symfony\Component\Messenger\Transport\SetupableTransportInterface;
use Symfony\Component\Messenger\Transport\TransportFactoryInterface;
use Symfony\Component\Messenger\Transport\TransportInterface;
use Symfony\Component\Messenger\Worker;
use Symfony\Component\Uid\Uuid;

/**
 * Spec (hardening): failure-mode behavior over real infrastructure.
 *
 * - Broker down at dispatch: without an outbox the failure surfaces at the caller
 *   (documented no-outbox trade-off); with an outbox the message is absorbed, the
 *   relay records the failed attempt, and everything drains after broker recovery.
 * - Database gone mid-handle: server-side rollback, the inbox row survives, and after
 *   redeliver_timeout the message is redelivered and handled effectively once.
 *
 * A worker killed between inbox insert and broker ack is the duplicate-delivery case,
 * covered by InboxReceiveRelayTest/InboxTransportTest (dedup drops the redelivery).
 * "Broker down" is simulated by pointing MWF_CHAOS_AMQP_DSN at a closed port —
 * "recovery" boots a fresh kernel against the real broker.
 */
#[Group('postgres')]
#[Group('rabbitmq')]
#[RequiresPhpExtension('pdo_pgsql')]
final class ChaosTest extends WorkflowKernelTestCase
{
    private const string DEAD_BROKER_DSN = 'phpamqplib://guest:guest@127.0.0.1:59999';
    private const string OUTBOX = 'chaos_outbox';
    private const string INBOX = 'chaos_commands';

    protected static function getKernelClass(): string
    {
        return ChaosKernel::class;
    }

    protected function setUp(): void
    {
        parent::setUp();

        Infra::requireAmqp()->close();
        $_ENV['MWF_CHAOS_AMQP_DSN'] = Infra::amqpDsn();

        ChaosCommandHandler::reset();

        // Clean slate via an observer session (kernels boot per test phase).
        $observer = PostgresDbal::createConnection();
        $observer->executeStatement('DROP TABLE IF EXISTS chaos_handled');
        $observer->executeStatement('CREATE TABLE chaos_handled (id BIGSERIAL PRIMARY KEY, payload TEXT NOT NULL)');
        foreach (['zz_chaos_outbox', 'zz_chaos_cmd_inbox', 'zz_chaos_cmd_inbox_index'] as $table) {
            $observer->executeStatement(\sprintf('DROP TABLE IF EXISTS "%s"', $table));
        }
        $observer->close();
    }

    protected function tearDown(): void
    {
        unset($_ENV['MWF_CHAOS_AMQP_DSN']);

        parent::tearDown();
    }

    private static function fetchInt(DbalConnection $connection, string $sql): int
    {
        $value = $connection->fetchOne($sql);
        self::assertIsNumeric($value);

        return (int) $value;
    }

    private function transport(string $name): TransportInterface
    {
        $transport = self::getContainer()->get('messenger.transport.'.$name);
        self::assertInstanceOf(TransportInterface::class, $transport);

        return $transport;
    }

    private function setupDatabaseTransports(): void
    {
        foreach ([self::OUTBOX, self::INBOX] as $name) {
            $transport = $this->transport($name);
            self::assertInstanceOf(SetupableTransportInterface::class, $transport);
            $transport->setup();
        }
    }

    private function messageCount(string $transportName): int
    {
        $transport = $this->transport($transportName);
        self::assertInstanceOf(MessageCountAwareInterface::class, $transport);

        return $transport->getMessageCount();
    }

    private function commandBus(): CommandBusInterface
    {
        $bus = self::getContainer()->get(CommandBusInterface::class);
        self::assertInstanceOf(CommandBusInterface::class, $bus);

        return $bus;
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
    private function runWorker(array $transportNames, string $busId, int $messageLimit, int $timeLimit = 10): void
    {
        $receivers = [];
        foreach ($transportNames as $name) {
            $receivers[$name] = $this->transport($name);
        }

        $this->runWorkerWith($receivers, $busId, $messageLimit, $timeLimit);
    }

    private function runQueueHandlerWorker(string $queue, int $messageLimit): void
    {
        $container = self::getContainer();
        $factory = $container->get('messenger.transport_factory');
        self::assertInstanceOf(TransportFactoryInterface::class, $factory);
        $serializer = $container->get('messenger.default_serializer');
        self::assertInstanceOf(SerializerInterface::class, $serializer);

        $transport = $factory->createTransport(Infra::amqpDsn(), [
            'transport_name' => 'commands',
            'auto_setup' => false,
            'exchange' => ['name' => 'commands', 'type' => 'direct'],
            'queues' => [$queue => []],
        ], $serializer);

        $this->runWorkerWith(['commands' => $transport], 'command.bus', $messageLimit);
    }

    private function setupBrokerTopologyAndPurge(): void
    {
        $kernel = self::$kernel;
        self::assertNotNull($kernel);
        $application = new Application($kernel);
        $application->setAutoExit(false);
        $output = new BufferedOutput();
        self::assertSame(0, $application->run(new ArrayInput(['command' => 'messenger:setup-transports', '--no-interaction' => true]), $output), $output->fetch());

        $amqp = Infra::requireAmqp();
        $channel = $amqp->channel();
        $channel->queue_purge('chaos_commands');
        $channel->close();
        $amqp->close();
    }

    public function testBrokerDownAtDispatchWithoutOutboxSurfacesTheFailureAtTheCaller(): void
    {
        $_ENV['MWF_CHAOS_AMQP_DSN'] = self::DEAD_BROKER_DSN;
        self::bootKernel();
        $this->setupDatabaseTransports();

        $thrown = null;
        try {
            $this->commandBus()->dispatch(new ChaosCommand('unreachable'));
        } catch (\Throwable $thrown) {
        }

        self::assertNotNull($thrown, 'Without an outbox a broker outage must surface at dispatch time');
        self::assertSame(0, $this->messageCount(self::OUTBOX), 'Nothing was written anywhere');
        self::assertSame(0, ChaosCommandHandler::attempts('unreachable'));
    }

    public function testBrokerOutageIsAbsorbedByTheOutboxAndDrainedAfterRecovery(): void
    {
        // Step 1 — broker down: dispatch succeeds, the relay attempt fails softly.
        $_ENV['MWF_CHAOS_AMQP_DSN'] = self::DEAD_BROKER_DSN;
        self::bootKernel();
        $this->setupDatabaseTransports();

        $this->commandBus()->dispatch(new Envelope(new ChaosCommand('resilient'), [new TransportNamesStamp([self::OUTBOX])]));
        self::assertSame(1, $this->messageCount(self::OUTBOX), 'The outbox absorbed the dispatch while the broker is down');

        $this->runWorker([self::OUTBOX], 'relay.bus', 1);

        self::assertSame(1, $this->messageCount(self::OUTBOX), 'The failed relay attempt kept the row');
        $observer = PostgresDbal::createConnection();
        self::assertSame(1, self::fetchInt($observer, 'SELECT retry_count FROM zz_chaos_outbox'), 'The failed attempt was recorded on the row');

        // Step 2 — broker back: fresh kernel against the real broker drains the outbox.
        self::ensureKernelShutdown();
        $_ENV['MWF_CHAOS_AMQP_DSN'] = Infra::amqpDsn();
        self::bootKernel();
        $this->setupBrokerTopologyAndPurge();

        $this->runWorker([self::OUTBOX], 'relay.bus', 1);
        self::assertSame(0, $this->messageCount(self::OUTBOX), 'The relay published the absorbed message after recovery');

        $this->runQueueHandlerWorker('chaos_commands', 1);
        self::assertSame(1, ChaosCommandHandler::attempts('resilient'));
        self::assertSame(1, self::fetchInt($observer, 'SELECT COUNT(*) FROM chaos_handled'), 'The command took effect exactly once');
        $observer->close();
    }

    public function testDatabaseLossMidHandleRollsBackAndRedeliversExactlyOnce(): void
    {
        self::bootKernel();
        $this->setupDatabaseTransports();

        // Seed the inbox directly (the broker plays no part in this scenario).
        $this->transport(self::INBOX)->send(new Envelope(new ChaosCommand('kill-db-once'), [
            new MessageIdStamp((string) Uuid::v7()),
        ]));

        // Attempt 1: the handler writes, then its database backend is terminated.
        // The worker process itself may die with it — that is part of the scenario.
        try {
            $this->runWorker([self::INBOX], 'command.bus', 1, 5);
        } catch (\Throwable) {
        }

        $observer = PostgresDbal::createConnection();
        self::assertSame(1, ChaosCommandHandler::attempts('kill-db-once'), 'The first attempt ran');
        self::assertSame(0, self::fetchInt($observer, 'SELECT COUNT(*) FROM chaos_handled'), 'The write of the crashed attempt rolled back server-side');
        self::assertSame(1, self::fetchInt($observer, 'SELECT COUNT(*) FROM zz_chaos_cmd_inbox'), 'The inbox row survived the crash');

        // "Worker restart": drop the dead connection; DBAL reconnects on next use.
        $dbal = self::getContainer()->get('doctrine.dbal.postgres_connection');
        self::assertInstanceOf(DbalConnection::class, $dbal);
        $dbal->close();

        // After redeliver_timeout (1s; comparison truncates to whole seconds) the
        // in-flight marker expires and the row is delivered again.
        usleep(2_300_000);
        $this->runWorker([self::INBOX], 'command.bus', 1, 5);

        self::assertSame(2, ChaosCommandHandler::attempts('kill-db-once'));
        self::assertSame(1, self::fetchInt($observer, 'SELECT COUNT(*) FROM chaos_handled'), 'The redelivered message took effect exactly once');
        self::assertSame(0, self::fetchInt($observer, 'SELECT COUNT(*) FROM zz_chaos_cmd_inbox'), 'The inbox row was consumed transactionally on the successful attempt');
        $observer->close();
    }
}
