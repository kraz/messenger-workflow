<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Integration\Amqp;

use Jwage\PhpAmqpLibMessengerBundle\RetryFactory;
use Jwage\PhpAmqpLibMessengerBundle\Transport\AmqpConnectionFactory;
use Jwage\PhpAmqpLibMessengerBundle\Transport\AmqpReceivedStamp;
use Jwage\PhpAmqpLibMessengerBundle\Transport\ConnectionFactory;
use Jwage\PhpAmqpLibMessengerBundle\Transport\DsnParser;
use Kraz\MessengerWorkflow\Application\Exception\TaskTimeOutException;
use Kraz\MessengerWorkflow\Messenger\Amqp\AmqpTransport;
use Kraz\MessengerWorkflow\Messenger\Stamp\AsyncMessageStamp;
use Kraz\MessengerWorkflow\Messenger\Stamp\AwaitMessageResultStamp;
use Kraz\MessengerWorkflow\Messenger\Stamp\CommandStamp;
use Kraz\MessengerWorkflow\Messenger\Stamp\EventStamp;
use Kraz\MessengerWorkflow\Messenger\Stamp\ResultStorageStamp;
use Kraz\MessengerWorkflow\Messenger\Stamp\StrictOrderStamp;
use Kraz\MessengerWorkflow\Messenger\Stamp\TargetTransportNameStamp;
use Kraz\MessengerWorkflow\Messenger\Transport\InMemoryResultStorage;
use Kraz\MessengerWorkflow\Tests\Fixture\Message\TestCommand;
use Kraz\MessengerWorkflow\Tests\Fixture\Message\TestEvent;
use PHPUnit\Framework\Attributes\Group;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Stamp\HandledStamp;
use Symfony\Component\Messenger\Transport\Serialization\PhpSerializer;
use Symfony\Component\Uid\Uuid;

/**
 * Spec: RabbitMQ integration via jwage/phpamqplib-messenger — direct exchange for commands,
 * topic exchange for events, message UUID as AMQP message_id, queue name mapped to the
 * target (inbox) transport, async/await task semantics.
 *
 * Runs against the live RabbitMQ from README (localhost:5672, guest/guest) using
 * test-scoped exchange/queue names, cleaned up afterwards.
 */
#[Group('rabbitmq')]
final class AmqpTransportTest extends TestCase
{
    // Test-scoped names, unique per test: a queue shared between tests would round-robin
    // deliveries to stale consumers left over from earlier test instances.
    private string $suffix;
    private string $exchangeCommands;
    private string $exchangeEvents;
    private string $queueCommands;
    private string $queueEvents;

    private InMemoryResultStorage $resultStorage;
    private ?AmqpTransport $commands = null;
    private ?AmqpTransport $events = null;

    /** @var \Jwage\PhpAmqpLibMessengerBundle\Transport\Connection[] */
    private array $openConnections = [];

    protected function setUp(): void
    {
        $this->suffix = bin2hex(random_bytes(4));
        $this->exchangeCommands = 'mwf_test_commands_'.$this->suffix;
        $this->exchangeEvents = 'mwf_test_events_'.$this->suffix;
        $this->queueCommands = 'mwf_test_app_commands_'.$this->suffix;
        $this->queueEvents = 'mwf_test_app_events_'.$this->suffix;

        $this->resultStorage = new InMemoryResultStorage();
        // fail fast when the broker is down
        try {
            $this->commandsTransport()->getMessageCount();
        } catch (\Throwable $e) {
            self::markTestSkipped('RabbitMQ is not reachable: '.$e->getMessage());
        }
    }

    protected function tearDown(): void
    {
        // Close the transports' broker connections eagerly: a socket left open here would be
        // inherited by later forking tests (RedisResultStorageTest), whose child process would
        // send connection.close on the shared socket and make the parent's shutdown hang on a
        // dead connection until the read timeout.
        foreach ($this->openConnections as $connection) {
            try {
                $connection->close();
            } catch (\Throwable) {
                // already closed / broker gone
            }
        }
        $this->openConnections = [];

        try {
            $channel = new \PhpAmqpLib\Connection\AMQPStreamConnection(
                parse_url($this->dsn(), \PHP_URL_HOST) ?: '127.0.0.1',
                parse_url($this->dsn(), \PHP_URL_PORT) ?: 5672,
                parse_url($this->dsn(), \PHP_URL_USER) ?: 'guest',
                parse_url($this->dsn(), \PHP_URL_PASS) ?: 'guest',
            )->channel();
            foreach ([$this->queueCommands, $this->queueEvents] as $queue) {
                $channel->queue_delete($queue);
            }
            foreach ([$this->exchangeCommands, $this->exchangeEvents] as $exchange) {
                $channel->exchange_delete($exchange);
            }
            $channel->getConnection()?->close();
        } catch (\Throwable) {
            // broker unavailable — nothing to clean up
        }
    }

    private function dsn(): string
    {
        return $_ENV['MWF_TEST_AMQP_DSN'] ?? 'phpamqplib://guest:guest@127.0.0.1:5672';
    }

    private function createTransport(array $options): AmqpTransport
    {
        $factory = new ConnectionFactory(new DsnParser(), new RetryFactory(), new AmqpConnectionFactory());
        $connection = $factory->fromDsn($this->dsn(), $options + ['auto_setup' => true]);
        $this->openConnections[] = $connection;

        return new AmqpTransport(
            connection: $connection,
            serializer: new PhpSerializer(),
            resultStorage: $this->resultStorage,
        );
    }

    private function commandsTransport(): AmqpTransport
    {
        // Spec: "Commands/Queries use direct exchange type"
        // One instance per test: a second transport would register a competing consumer.
        return $this->commands ??= $this->createTransport([
            'exchange' => ['name' => $this->exchangeCommands, 'type' => 'direct'],
            'queues' => [
                $this->queueCommands => [
                    'binding_keys' => ['commands.internal.Kraz'],
                ],
            ],
        ]);
    }

    private function eventsTransport(): AmqpTransport
    {
        // Spec: "Events use topic exchange type"
        return $this->events ??= $this->createTransport([
            'exchange' => ['name' => $this->exchangeEvents, 'type' => 'topic'],
            'queues' => [
                $this->queueEvents => [
                    'binding_keys' => ['events.internal.Kraz.#'],
                ],
            ],
        ]);
    }

    /**
     * @return Envelope[]
     */
    private function fetch(AmqpTransport $transport, int $attempts = 20): array
    {
        for ($i = 0; $i < $attempts; ++$i) {
            $envelopes = iterator_to_array($transport->get(1), false);
            if ([] !== $envelopes) {
                return $envelopes;
            }
            usleep(100_000);
        }

        return [];
    }

    public function testCommandIsPublishedWithMessageIdRoutingKeyAndPriority(): void
    {
        $transport = $this->commandsTransport();
        $messageId = (string) Uuid::v7();

        $transport->send(new Envelope(new TestCommand('over-amqp'), [
            new CommandStamp($messageId, 'commands.internal.Kraz', priority: 3),
        ]));

        $envelopes = $this->fetch($transport);
        self::assertCount(1, $envelopes);
        $received = $envelopes[0];

        self::assertInstanceOf(TestCommand::class, $received->getMessage());
        self::assertSame('over-amqp', $received->getMessage()->payload);

        $amqpStamp = $received->last(AmqpReceivedStamp::class);
        self::assertNotNull($amqpStamp);
        self::assertSame($messageId, $amqpStamp->getAmqpEnvelope()->getMessageId());
        self::assertSame('commands.internal.Kraz', $amqpStamp->getAmqpEnvelope()->getRoutingKey());
        self::assertSame(3, $amqpStamp->getAmqpEnvelope()->getPriority());

        // Spec: the receiver worker maps the queue name to the target inbox transport
        self::assertSame($this->queueCommands, $received->last(TargetTransportNameStamp::class)?->getTransportName());

        $transport->ack($received);
    }

    public function testEventIsRoutedThroughTopicExchange(): void
    {
        $transport = $this->eventsTransport();
        $messageId = (string) Uuid::v7();

        $transport->send(new Envelope(new TestEvent('broadcast'), [
            new EventStamp($messageId, 'events.internal.Kraz.MessengerWorkflow.Tests.Fixture.Message.TestEvent'),
        ]));

        $envelopes = $this->fetch($transport);
        self::assertCount(1, $envelopes);
        self::assertInstanceOf(TestEvent::class, $envelopes[0]->getMessage());
        self::assertSame($messageId, $envelopes[0]->last(AmqpReceivedStamp::class)?->getAmqpEnvelope()->getMessageId());

        $transport->ack($envelopes[0]);
    }

    public function testAsyncSendReturnsTaskIdAndMarksResultStorage(): void
    {
        // Spec: "Async mode: caller polls by UUID"
        $transport = $this->commandsTransport();
        $messageId = (string) Uuid::v7();

        $result = $transport->send(new Envelope(new TestCommand(), [
            new CommandStamp($messageId, 'commands.internal.Kraz'),
            new AsyncMessageStamp(),
        ]));

        self::assertSame($messageId, $result->last(HandledStamp::class)?->getResult());
        self::assertNotNull($result->last(ResultStorageStamp::class));

        // the message really went to the broker
        $envelopes = $this->fetch($transport);
        self::assertCount(1, $envelopes);
        self::assertNotNull($envelopes[0]->last(ResultStorageStamp::class));
        $transport->ack($envelopes[0]);
    }

    public function testSyncSendAwaitsResultFromResultStorage(): void
    {
        // Spec: "Sync mode: caller can wait for task completion"
        $transport = $this->commandsTransport();
        $messageId = (string) Uuid::v7();
        $this->resultStorage->write($messageId, 'remote-result');

        $result = $transport->send(new Envelope(new TestCommand(), [
            new CommandStamp($messageId, 'commands.internal.Kraz'),
            new AwaitMessageResultStamp(2),
        ]));

        self::assertSame('remote-result', $result->last(HandledStamp::class)?->getResult());
    }

    public function testSyncSendTimesOutWithoutResult(): void
    {
        $transport = $this->commandsTransport();

        $this->expectException(TaskTimeOutException::class);

        $transport->send(new Envelope(new TestCommand(), [
            new CommandStamp((string) Uuid::v7(), 'commands.internal.Kraz'),
            new AwaitMessageResultStamp(1),
        ]));
    }

    public function testDeferredAwaitDoesNotPublishTheMessage(): void
    {
        // Used by CommandBus::await()/QueryBus::await(): wait for an already-dispatched task
        $transport = $this->commandsTransport();
        $messageId = (string) Uuid::v7();
        $this->resultStorage->write($messageId, 'already-running-task-result');

        $result = $transport->send(new Envelope(new TestCommand(), [
            new CommandStamp($messageId, 'commands.internal.Kraz'),
            new AwaitMessageResultStamp(2, deferred: true),
        ]));

        self::assertSame('already-running-task-result', $result->last(HandledStamp::class)?->getResult());
        self::assertSame(0, $transport->getMessageCount(), 'Deferred await must not publish a new message');
    }

    public function testRejectWithStrictOrderRequeuesTheMessage(): void
    {
        $transport = $this->commandsTransport();

        $transport->send(new Envelope(new TestCommand('requeue-me'), [
            new CommandStamp((string) Uuid::v7(), 'commands.internal.Kraz'),
            new StrictOrderStamp(),
        ]));

        [$received] = $this->fetch($transport);
        self::assertNotNull($received->last(StrictOrderStamp::class), 'StrictOrderStamp must survive the broker round-trip');

        $transport->reject($received);

        $requeued = $this->fetch($transport);
        self::assertCount(1, $requeued, 'nack(requeue) must put the message back');
        self::assertSame('requeue-me', $requeued[0]->getMessage()->payload);
        $transport->ack($requeued[0]);
    }

    public function testRejectWithoutStrictOrderDropsTheMessage(): void
    {
        $transport = $this->commandsTransport();

        $transport->send(new Envelope(new TestCommand('drop-me'), [
            new CommandStamp((string) Uuid::v7(), 'commands.internal.Kraz'),
        ]));

        [$received] = $this->fetch($transport);
        $transport->reject($received);

        self::assertSame([], $this->fetch($transport, 5));
    }
}
