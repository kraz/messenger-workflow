<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Integration\Doctrine;

use Contracts\Demo\Command\DoSomethingCommand;
use Jwage\PhpAmqpLibMessengerBundle\Transport\AmqpTransport;
use Kraz\MessengerWorkflow\Infrastructure\Doctrine\Inbox\InboxTransport;
use Kraz\MessengerWorkflow\Infrastructure\Messenger\Stamp\MessageIdStamp;
use Kraz\MessengerWorkflow\Tests\Support\Infra;
use Kraz\MessengerWorkflow\Tests\Support\WorkflowKernelTestCase;
use PHPUnit\Framework\Attributes\Group;
use PHPUnit\Framework\Attributes\RequiresPhpExtension;
use Symfony\Bundle\FrameworkBundle\Console\Application;
use Symfony\Component\Console\Input\ArrayInput;
use Symfony\Component\Console\Output\BufferedOutput;
use Symfony\Component\EventDispatcher\EventDispatcher;
use Symfony\Component\Messenger\EventListener\StopWorkerOnMessageLimitListener;
use Symfony\Component\Messenger\MessageBusInterface;
use Symfony\Component\Messenger\Worker;
use Symfony\Component\Uid\Uuid;

/**
 * Spec: FULL receiver segment — broker queue → receiver worker (inbox.bus) → receiver
 * inbox, with duplicate broker deliveries deduplicated by message UUID.
 */
#[Group('postgres')]
#[Group('rabbitmq')]
#[RequiresPhpExtension('pdo_pgsql')]
final class InboxReceiveRelayTest extends WorkflowKernelTestCase
{
    protected function setUp(): void
    {
        parent::setUp();

        $amqp = Infra::requireAmqp();
        $amqp->close();

        self::bootKernel();

        // Clean slate for the shared inbox tables, then re-provision everything.
        $dbal = self::getContainer()->get('doctrine.dbal.postgres_connection');
        self::assertInstanceOf(\Doctrine\DBAL\Connection::class, $dbal);
        try {
            $dbal->executeQuery('SELECT 1');
        } catch (\Throwable $e) {
            self::markTestSkipped('PostgreSQL is not reachable: '.$e->getMessage());
        }
        foreach (['zz_commands_inbox', 'zz_commands_inbox_index', 'zz_commands_failures'] as $table) {
            $dbal->executeStatement(\sprintf('DROP TABLE IF EXISTS "%s"', $table));
        }

        $kernel = self::$kernel;
        self::assertNotNull($kernel);
        $application = new Application($kernel);
        $application->setAutoExit(false);
        $output = new BufferedOutput();
        self::assertSame(0, $application->run(new ArrayInput(['command' => 'messenger:setup-transports', '--no-interaction' => true]), $output), $output->fetch());

        $amqp = Infra::requireAmqp();
        $channel = $amqp->channel();
        $channel->queue_purge('app_commands');
        $channel->close();
        $amqp->close();
    }

    protected function tearDown(): void
    {
        if (null !== self::$kernel) {
            $transport = self::getContainer()->get('messenger.transport.commands');
            if ($transport instanceof AmqpTransport) {
                $transport->getConnection()->close();
            }
        }

        parent::tearDown();
    }

    public function testBrokerMessagesLandInTheInboxWithDuplicatesDropped(): void
    {
        $commandBus = self::getContainer()->get('command.bus');
        self::assertInstanceOf(MessageBusInterface::class, $commandBus);

        // The same message delivered twice by the broker (same message UUID).
        $uuid = (string) Uuid::v7();
        $commandBus->dispatch(new DoSomethingCommand('dup'), [new MessageIdStamp($uuid)]);
        $commandBus->dispatch(new DoSomethingCommand('dup'), [new MessageIdStamp($uuid)]);

        // Receiver worker: broker queue app_commands → inbox transport app_commands.
        $brokerTransport = self::getContainer()->get('messenger.transport.commands');
        self::assertInstanceOf(AmqpTransport::class, $brokerTransport);
        $inboxBus = self::getContainer()->get('inbox.bus');
        self::assertInstanceOf(MessageBusInterface::class, $inboxBus);

        $dispatcher = new EventDispatcher();
        $dispatcher->addSubscriber(new StopWorkerOnMessageLimitListener(2));
        new Worker(['commands' => $brokerTransport], $inboxBus, $dispatcher)->run(['sleep' => 100000]);

        $inbox = self::getContainer()->get('messenger.transport.app_commands');
        self::assertInstanceOf(InboxTransport::class, $inbox);
        self::assertSame(1, $inbox->getMessageCount(), 'Duplicate broker delivery must be deduplicated');

        $envelopes = iterator_to_array($inbox->get(), false);
        self::assertCount(1, $envelopes);
        $message = $envelopes[0]->getMessage();
        self::assertInstanceOf(DoSomethingCommand::class, $message);
        self::assertSame('dup', $message->payload);
        self::assertSame($uuid, $envelopes[0]->last(MessageIdStamp::class)?->getMessageId());

        $inbox->ack($envelopes[0]);
    }
}
