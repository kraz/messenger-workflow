<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Integration\Doctrine;

use Contracts\Demo\Event\SomethingHappened;
use Jwage\PhpAmqpLibMessengerBundle\Transport\AmqpReceivedStamp;
use Kraz\MessengerWorkflow\Infrastructure\Doctrine\Outbox\OutboxTransport;
use Kraz\MessengerWorkflow\Infrastructure\Messenger\Stamp\MessageIdStamp;
use Kraz\MessengerWorkflow\Infrastructure\Messenger\Stamp\StrictOrderStamp;
use Kraz\MessengerWorkflow\Tests\Support\Infra;
use Kraz\MessengerWorkflow\Tests\Support\WorkflowKernelTestCase;
use PHPUnit\Framework\Attributes\Group;
use PHPUnit\Framework\Attributes\RequiresPhpExtension;
use Symfony\Bundle\FrameworkBundle\Console\Application;
use Symfony\Component\Console\Input\ArrayInput;
use Symfony\Component\Console\Output\BufferedOutput;
use Symfony\Component\EventDispatcher\EventDispatcher;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\EventListener\StopWorkerOnMessageLimitListener;
use Symfony\Component\Messenger\MessageBusInterface;
use Symfony\Component\Messenger\Stamp\TransportNamesStamp;
use Symfony\Component\Messenger\Transport\Receiver\QueueReceiverInterface;
use Symfony\Component\Messenger\Worker;

/**
 * Spec: FULL event publisher segment — publish() → outbox (PostgreSQL) → publisher
 * worker (relay.bus) → RabbitMQ topic exchange → bound queue.
 */
#[Group('postgres')]
#[Group('rabbitmq')]
#[RequiresPhpExtension('pdo_pgsql')]
final class OutboxRelayTest extends WorkflowKernelTestCase
{
    protected function setUp(): void
    {
        parent::setUp();

        // Both infra services must be reachable.
        $amqp = Infra::requireAmqp();
        $channel = $amqp->channel();
        $channel->close();
        $amqp->close();

        self::bootKernel();

        $kernel = self::$kernel;
        self::assertNotNull($kernel);
        $application = new Application($kernel);
        $application->setAutoExit(false);
        $output = new BufferedOutput();
        self::assertSame(0, $application->run(new ArrayInput(['command' => 'messenger:setup-transports', '--no-interaction' => true]), $output), $output->fetch());

        // Clean slate: drain the outbox table and purge the broker queue.
        $outbox = $this->outboxTransport();
        foreach ($outbox->all() as $envelope) {
            $outbox->reject($envelope); // keeps rows; delete via ack below
        }
        foreach ($outbox->get(100) as $envelope) {
            $outbox->ack($envelope);
        }
        $amqp = Infra::requireAmqp();
        $channel = $amqp->channel();
        $channel->queue_purge('app_events');
        $channel->close();
        $amqp->close();
    }

    private function outboxTransport(): OutboxTransport
    {
        $transport = self::getContainer()->get('messenger.transport.app_outbox');
        self::assertInstanceOf(OutboxTransport::class, $transport);

        return $transport;
    }

    public function testPublishedEventsAreRelayedFifoToTheBrokerQueue(): void
    {
        $eventBus = self::getContainer()->get('event.bus');
        self::assertInstanceOf(MessageBusInterface::class, $eventBus);

        // Publish through the outbox (TransportNamesStamp overrides the broker routing).
        $first = $eventBus->dispatch(new SomethingHappened('first'), [new TransportNamesStamp(['app_outbox'])]);
        $eventBus->dispatch(new SomethingHappened('second'), [new TransportNamesStamp(['app_outbox'])]);

        $firstMessageId = $first->last(MessageIdStamp::class);
        self::assertNotNull($firstMessageId);
        self::assertSame(2, $this->outboxTransport()->getMessageCount(), 'Rows persisted in the outbox');

        // Run the publisher (relay) worker for exactly two messages.
        $relayBus = self::getContainer()->get('relay.bus');
        self::assertInstanceOf(MessageBusInterface::class, $relayBus);
        $dispatcher = new EventDispatcher();
        $dispatcher->addSubscriber(new StopWorkerOnMessageLimitListener(2));
        new Worker(['app_outbox' => $this->outboxTransport()], $relayBus, $dispatcher)->run(['sleep' => 100000]);

        self::assertSame(0, $this->outboxTransport()->getMessageCount(), 'Relayed rows are acked (deleted)');

        // The messages arrived on the bound broker queue, in order, with their identity.
        $events = self::getContainer()->get('messenger.transport.events');
        self::assertInstanceOf(QueueReceiverInterface::class, $events);

        $received = [];
        $deadline = microtime(true) + 5.0;
        while (\count($received) < 2 && microtime(true) < $deadline) {
            foreach ($events->getFromQueues(['app_events']) as $envelope) {
                $received[] = $envelope;
                $events->ack($envelope);
            }
        }

        self::assertCount(2, $received);
        $payloads = array_map(static function (Envelope $envelope): string {
            $message = $envelope->getMessage();

            return $message instanceof SomethingHappened ? $message->payload : '?';
        }, $received);
        self::assertSame(['first', 'second'], $payloads, 'FIFO order preserved through the relay');

        self::assertSame($firstMessageId->getMessageId(), $received[0]->last(MessageIdStamp::class)?->getMessageId());
        self::assertNotNull($received[0]->last(StrictOrderStamp::class));

        $amqpStamp = $received[0]->last(AmqpReceivedStamp::class);
        self::assertNotNull($amqpStamp);
        self::assertSame('events.Demo.Event.SomethingHappened', $amqpStamp->getAmqpEnvelope()->getRoutingKey());
        self::assertSame($firstMessageId->getMessageId(), $amqpStamp->getAmqpEnvelope()->getMessageId());
    }
}
