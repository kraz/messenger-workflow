<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Integration\Flow;

use Contracts\Demo\Event\GammaEvent;
use Contracts\Demo\Event\OrderedEvent;
use Contracts\Demo\Event\SomethingHappened;
use Doctrine\DBAL\Connection as DbalConnection;
use Kraz\MessengerWorkflow\Application\Messenger\EventBusInterface;
use Kraz\MessengerWorkflow\Domain\OutboxBusInterface;
use Kraz\MessengerWorkflow\Tests\Fixture\Handler\BetaEventHandler;
use Kraz\MessengerWorkflow\Tests\Fixture\Handler\ContractEventFromTransportHandler;
use Kraz\MessengerWorkflow\Tests\Fixture\Handler\GammaTransactionalEventHandler;
use Kraz\MessengerWorkflow\Tests\Fixture\Handler\OrderedEventsHandler;
use Kraz\MessengerWorkflow\Tests\Fixture\MessageRecorder;
use Kraz\MessengerWorkflow\Tests\Support\Infra;
use Kraz\MessengerWorkflow\Tests\Support\WorkflowKernelTestCase;
use Kraz\MessengerWorkflow\Tests\TestKernel\EventFlowKernel;
use PHPUnit\Framework\Attributes\Group;
use PHPUnit\Framework\Attributes\RequiresPhpExtension;
use Symfony\Bundle\FrameworkBundle\Console\Application;
use Symfony\Component\Console\Input\ArrayInput;
use Symfony\Component\Console\Output\BufferedOutput;
use Symfony\Component\EventDispatcher\EventDispatcherInterface;
use Symfony\Component\Messenger\EventListener\StopWorkerOnMessageLimitListener;
use Symfony\Component\Messenger\EventListener\StopWorkerOnTimeLimitListener;
use Symfony\Component\Messenger\MessageBusInterface;
use Jwage\PhpAmqpLibMessengerBundle\Transport\AmqpTransport;
use Symfony\Component\Messenger\Transport\Receiver\MessageCountAwareInterface;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;
use Symfony\Component\Messenger\Transport\TransportFactoryInterface;
use Symfony\Component\Messenger\Transport\TransportInterface;
use Symfony\Component\Messenger\Worker;

/**
 * Spec: event flow end-to-end over real infrastructure — publish → outbox →
 * RabbitMQ topic exchange → per-context inboxes (different databases) → handlers;
 * fromTransport scoping; FIFO ordering with a poison message draining to the DLQ and
 * the queue resuming; optional transactional_handler mode; direct publish (no outbox).
 */
#[Group('postgres')]
#[Group('rabbitmq')]
#[RequiresPhpExtension('pdo_pgsql')]
final class EventFlowTest extends WorkflowKernelTestCase
{
    private const string OUTBOX = 'app_outbox';
    private const string BROKER = 'events';

    protected static function getKernelClass(): string
    {
        return EventFlowKernel::class;
    }

    protected function setUp(): void
    {
        parent::setUp();

        $amqp = Infra::requireAmqp();

        self::bootKernel();
        $kernel = self::$kernel;
        self::assertNotNull($kernel);
        $application = new Application($kernel);
        $application->setAutoExit(false);
        $output = new BufferedOutput();
        self::assertSame(0, $application->run(new ArrayInput(['command' => 'messenger:setup-transports', '--no-interaction' => true]), $output), $output->fetch());

        // Clean slate on both databases and the broker.
        $postgres = $this->connection('postgres');
        foreach (['zz_events_outbox', 'zz_events_inbox', 'zz_events_inbox_index', 'zz_gamma_inbox', 'zz_gamma_inbox_index', 'zz_events_failures'] as $table) {
            $postgres->executeStatement(\sprintf('TRUNCATE TABLE "%s"', $table));
        }
        $sqlite = $this->connection('default');
        foreach (['zz_events_inbox', 'zz_events_inbox_index', 'zz_events_failures'] as $table) {
            $sqlite->executeStatement(\sprintf('DELETE FROM "%s"', $table));
        }
        $channel = $amqp->channel();
        foreach (['app_events', 'beta_events', 'gamma_events'] as $queue) {
            $channel->queue_purge($queue);
        }
        $channel->close();
        $amqp->close();

        OrderedEventsHandler::reset();
        BetaEventHandler::reset();
        GammaTransactionalEventHandler::reset();
    }

    private function connection(string $name): DbalConnection
    {
        $connection = self::getContainer()->get('doctrine.dbal.'.$name.'_connection');
        self::assertInstanceOf(DbalConnection::class, $connection);

        return $connection;
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

    /**
     * Receiver worker for ONE broker queue, mirroring production: the jwage transport
     * holds a single AMQP consumer bound to the first queue it consumes, so every
     * receiver worker is queue-scoped ("messenger:consume events --queues=<q>") and
     * gets its own transport instance/connection (closed afterwards — a dangling
     * prefetch consumer would steal messages from later workers).
     */
    private function runQueueReceiver(string $queue, int $messageLimit): void
    {
        $container = self::getContainer();
        $factory = $container->get('messenger.transport_factory');
        self::assertInstanceOf(TransportFactoryInterface::class, $factory);
        $serializer = $container->get('messenger.default_serializer');
        self::assertInstanceOf(SerializerInterface::class, $serializer);

        $transport = $factory->createTransport(Infra::amqpDsn(), [
            'transport_name' => self::BROKER,
            'auto_setup' => false,
            'exchange' => ['name' => 'events', 'type' => 'topic'],
            'queues' => [$queue => []],
        ], $serializer);

        try {
            $this->runWorkerWith([self::BROKER => $transport], 'inbox.bus', $messageLimit);
        } finally {
            self::assertInstanceOf(AmqpTransport::class, $transport);
            $transport->getConnection()->close();
        }
    }

    private function outboxBus(): OutboxBusInterface
    {
        $bus = self::getContainer()->get(OutboxBusInterface::class);
        self::assertInstanceOf(OutboxBusInterface::class, $bus);

        return $bus;
    }

    private function recorder(): MessageRecorder
    {
        $recorder = self::getContainer()->get(MessageRecorder::class);
        self::assertInstanceOf(MessageRecorder::class, $recorder);

        return $recorder;
    }

    public function testFanOutToTwoContextsOnTwoDatabasesWithFromTransportScoping(): void
    {
        // Published through the generic per-context outbox bus (OutboxBusInterface).
        $event = new SomethingHappened('fan-out');
        $this->outboxBus()->publish($event);

        self::assertSame(1, $this->messageCount(self::OUTBOX), 'The outbox bus stored the event in app_outbox');

        $this->runWorker([self::OUTBOX], 'relay.bus', 1);

        // The topic exchange fans the message out to both bound queues; one
        // queue-scoped receiver worker per context moves it into that context's inbox.
        $this->runQueueReceiver('app_events', 1);
        $this->runQueueReceiver('beta_events', 1);
        self::assertSame(1, $this->messageCount('app_events'), 'Kraz context inbox (postgres) received a copy');
        self::assertSame(1, $this->messageCount('beta_events'), 'Beta context inbox (sqlite) received a copy');

        $this->runWorker(['app_events'], 'event.bus', 1);
        $this->runWorker(['beta_events'], 'event.bus', 1);

        self::assertSame(['fan-out'], BetaEventHandler::$received, 'The beta handler fired exactly once — for the beta_events delivery only');
        self::assertSame(
            [ContractEventFromTransportHandler::class],
            $this->recorder()->handlerNames(),
            'The app context handler fired exactly once — fromTransport scoping kept it away from the beta delivery',
        );
        self::assertSame(0, $this->messageCount('app_events'));
        self::assertSame(0, $this->messageCount('beta_events'));
    }

    public function testOrderingIsPreservedAndAPoisonMessageDrainsToTheDlqThenTheQueueResumes(): void
    {
        $bus = $this->outboxBus();
        foreach (['e1', 'poison-x', 'e2', 'e3'] as $payload) {
            $bus->publish(new OrderedEvent($payload));
        }

        $this->runWorker([self::OUTBOX], 'relay.bus', 4);
        $this->runQueueReceiver('app_events', 4);
        self::assertSame(4, $this->messageCount('app_events'));

        // 3 successes + 3 poison attempts (initial + max_retries=2), FIFO order:
        // the retrying poison message blocks the ordered queue until it exhausts
        // its budget and is moved to the failure transport — then the queue resumes.
        $this->runWorker(['app_events'], 'event.bus', 6);

        self::assertSame(
            ['e1', 'poison-x', 'poison-x', 'poison-x', 'e2', 'e3'],
            OrderedEventsHandler::$invocations,
            'FIFO order; the poison message blocked the queue for exactly its retry budget',
        );
        self::assertSame(1, $this->messageCount('app_events_failures'), 'The poison message ended in the DLQ');
        self::assertSame(0, $this->messageCount('app_events'), 'The queue drained after the poison message was removed');
        self::assertNotContains(true, OrderedEventsHandler::$inTransaction, 'Events inboxes are non-transactional by default');
    }

    public function testTransactionalHandlerModeRunsEventHandlersInsideTheInboxTransaction(): void
    {
        // Direct publish — no outbox involved (reduced-flow variant).
        $eventBus = self::getContainer()->get(EventBusInterface::class);
        self::assertInstanceOf(EventBusInterface::class, $eventBus);
        $eventBus->publish(new GammaEvent('tx'));

        self::assertSame(0, $this->messageCount(self::OUTBOX), 'Direct publish bypasses the outbox');

        $this->runQueueReceiver('gamma_events', 1);
        self::assertSame(1, $this->messageCount('gamma_events'));

        $this->runWorker(['gamma_events'], 'event.bus', 1);

        self::assertSame([true], GammaTransactionalEventHandler::$inTransaction, 'transactional_handler=true: the handler ran inside the inbox transaction');
        self::assertSame(0, $this->messageCount('gamma_events'));
    }
}
