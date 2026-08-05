<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Integration\Amqp;

use Contracts\Demo\Command\DoSomethingCommand;
use Contracts\Demo\Event\OtherThingHappened;
use Contracts\Demo\Event\SomethingHappened;
use Contracts\Demo\Query\GetSomethingQuery;
use Jwage\PhpAmqpLibMessengerBundle\Transport\AmqpReceivedStamp;
use Kraz\MessengerWorkflow\Infrastructure\Messenger\Stamp\MessageIdStamp;
use Kraz\MessengerWorkflow\Tests\Fixture\Message\TestEvent;
use Kraz\MessengerWorkflow\Tests\Support\Infra;
use Kraz\MessengerWorkflow\Tests\Support\WorkflowKernelTestCase;
use PHPUnit\Framework\Attributes\Group;
use Symfony\Bundle\FrameworkBundle\Console\Application;
use Symfony\Component\Console\Input\ArrayInput;
use Symfony\Component\Console\Output\BufferedOutput;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\MessageBusInterface;
use Symfony\Component\Messenger\Transport\Receiver\QueueReceiverInterface;

/**
 * Spec: RabbitMQ integration through the stock jwage transport — routing keys attached by
 * bus middleware, queue bindings derived by the compiler pass, topology provisioned by
 * messenger:setup-transports, queue-scoped consumption via QueueReceiverInterface.
 */
#[Group('rabbitmq')]
final class AmqpTransportTest extends WorkflowKernelTestCase
{
    protected function tearDown(): void
    {
        // Close the kernel's AMQP connections: dangling consumers (prefetch=1) would
        // otherwise steal messages published by later tests in this process.
        if (null !== self::$kernel) {
            foreach (['commands', 'queries', 'events'] as $transportName) {
                $transport = self::getContainer()->get('messenger.transport.'.$transportName);
                if ($transport instanceof \Jwage\PhpAmqpLibMessengerBundle\Transport\AmqpTransport) {
                    $transport->getConnection()->close();
                }
            }
        }

        parent::tearDown();
    }

    protected function setUp(): void
    {
        parent::setUp();

        // Reset the broker topology so binding assertions are deterministic.
        $connection = Infra::requireAmqp();
        $channel = $connection->channel();
        foreach (['app_commands', 'app_queries', 'app_events'] as $queue) {
            $channel->queue_delete($queue);
        }
        foreach (['commands', 'queries', 'events', 'commands.delay', 'queries.delay', 'delays'] as $exchange) {
            $channel->exchange_delete($exchange);
        }
        $channel->close();
        $connection->close();

        self::bootKernel();

        $kernel = self::$kernel;
        self::assertNotNull($kernel);
        $application = new Application($kernel);
        $application->setAutoExit(false);
        $output = new BufferedOutput();
        $exitCode = $application->run(new ArrayInput(['command' => 'messenger:setup-transports', '--no-interaction' => true]), $output);
        self::assertSame(0, $exitCode, 'messenger:setup-transports failed: '.$output->fetch());
    }

    private function bus(string $busId): MessageBusInterface
    {
        $bus = self::getContainer()->get($busId);
        self::assertInstanceOf(MessageBusInterface::class, $bus);

        return $bus;
    }

    /**
     * @return list<Envelope>
     */
    private function receiveAll(string $transportName, string $queueName, float $timeoutSeconds = 5.0): array
    {
        $transport = self::getContainer()->get('messenger.transport.'.$transportName);
        self::assertInstanceOf(QueueReceiverInterface::class, $transport);

        $envelopes = [];
        $deadline = microtime(true) + $timeoutSeconds;
        $idleRounds = 0;

        while (microtime(true) < $deadline && $idleRounds < 2) {
            $received = false;
            foreach ($transport->getFromQueues([$queueName]) as $envelope) {
                $envelopes[] = $envelope;
                $transport->ack($envelope);
                $received = true;
            }
            $idleRounds = $received ? 0 : $idleRounds + 1;
        }

        return $envelopes;
    }

    public function testCommandRoundTripOnTheDirectExchange(): void
    {
        $sent = $this->bus('command.bus')->dispatch(new DoSomethingCommand('hello'));
        $sentId = $sent->last(MessageIdStamp::class);
        self::assertNotNull($sentId);

        $envelopes = $this->receiveAll('commands', 'app_commands');
        self::assertCount(1, $envelopes);

        $envelope = $envelopes[0];
        $message = $envelope->getMessage();
        self::assertInstanceOf(DoSomethingCommand::class, $message);
        self::assertSame('hello', $message->payload);

        // The MessageIdStamp is serialized with the envelope and restored on receive.
        $receivedId = $envelope->last(MessageIdStamp::class);
        self::assertNotNull($receivedId);
        self::assertSame($sentId->getMessageId(), $receivedId->getMessageId());

        // The id is also propagated as the native AMQP message_id property.
        $amqpStamp = $envelope->last(AmqpReceivedStamp::class);
        self::assertNotNull($amqpStamp);
        self::assertSame($sentId->getMessageId(), $amqpStamp->getAmqpEnvelope()->getMessageId());
        self::assertSame('commands.Demo', $amqpStamp->getAmqpEnvelope()->getRoutingKey());
    }

    public function testQueryRoundTripOnTheDirectExchange(): void
    {
        $this->bus('query.bus')->dispatch(new GetSomethingQuery('q'));

        $envelopes = $this->receiveAll('queries', 'app_queries');
        self::assertCount(1, $envelopes);

        $amqpStamp = $envelopes[0]->last(AmqpReceivedStamp::class);
        self::assertNotNull($amqpStamp);
        self::assertSame('queries.Demo', $amqpStamp->getAmqpEnvelope()->getRoutingKey());
        self::assertInstanceOf(GetSomethingQuery::class, $envelopes[0]->getMessage());
    }

    public function testTopicExchangeDeliversOnlyBoundEvents(): void
    {
        $eventBus = $this->bus('event.bus');

        // Bound via the owner wildcard events.internal.Kraz.#:
        $eventBus->dispatch(new TestEvent('internal-1'));
        // Bound via the explicit binding key:
        $eventBus->dispatch(new SomethingHappened('contract-1'));
        // NOT bound anywhere — must not be delivered:
        $eventBus->dispatch(new OtherThingHappened());

        $messages = array_map(
            static fn (Envelope $envelope): object => $envelope->getMessage(),
            $this->receiveAll('events', 'app_events'),
        );

        self::assertCount(2, $messages);
        self::assertInstanceOf(TestEvent::class, $messages[0]);
        self::assertSame('internal-1', $messages[0]->payload);
        self::assertInstanceOf(SomethingHappened::class, $messages[1]);
        self::assertSame('contract-1', $messages[1]->payload);
    }
}
