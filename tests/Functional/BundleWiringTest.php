<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Functional;

use Kraz\MessengerWorkflow\Messenger\Amqp\AmqpTransport;
use Kraz\MessengerWorkflow\Messenger\CommandBus;
use Kraz\MessengerWorkflow\Messenger\Doctrine\Transport\Inbox\InboxTransport;
use Kraz\MessengerWorkflow\Messenger\Doctrine\Transport\Outbox\OutboxTransport;
use Kraz\MessengerWorkflow\Messenger\EventBus;
use Kraz\MessengerWorkflow\Messenger\QueryBus;
use Kraz\MessengerWorkflow\Messenger\Transport\InMemoryResultStorage;
use Kraz\MessengerWorkflow\Tests\Fixture\BusAccessor;
use PHPUnit\Framework\TestCase;
use Symfony\Bundle\FrameworkBundle\Console\Application;
use Kraz\MessengerWorkflow\Tests\Support\WorkflowKernelTestCase;
use Symfony\Component\Messenger\MessageBusInterface;
use Symfony\Component\Messenger\Transport\TransportInterface;

/**
 * Spec: "5 Symfony Messenger buses", transport factories for outbox/inbox/failures,
 * supervisor config command, RabbitMQ integration via jwage/phpamqplib-messenger.
 */
final class BundleWiringTest extends WorkflowKernelTestCase
{
    public function testAllFiveBusesAreRegistered(): void
    {
        $container = self::getContainer();

        foreach (['command.bus', 'query.bus', 'event.bus', 'outbox.bus', 'inbox.bus'] as $busId) {
            self::assertInstanceOf(MessageBusInterface::class, $container->get($busId), $busId);
        }
    }

    public function testBusInterfacesAreAutowirableLikeApplicationCode(): void
    {
        $accessor = self::getContainer()->get(BusAccessor::class);

        self::assertInstanceOf(CommandBus::class, $accessor->commandBus);
        self::assertInstanceOf(QueryBus::class, $accessor->queryBus);
        self::assertInstanceOf(EventBus::class, $accessor->eventBus);
    }

    public function testResultStorageDefaultsToMemoryProvider(): void
    {
        $storage = self::getContainer()->get('messenger_workflow.result_storage');

        self::assertInstanceOf(InMemoryResultStorage::class, $storage);
    }

    public function testOutboxAndInboxTransportFactoriesCreateTransportsFromDsn(): void
    {
        // Spec: "Usage of corresponding OutboxTransportFactory, InboxTransportFactory and
        // FailureTransportFactory"; DSN host maps to the DBAL connection name.
        $container = self::getContainer();

        self::assertInstanceOf(OutboxTransport::class, $container->get('messenger.transport.app_outbox'));
        self::assertInstanceOf(OutboxTransport::class, $container->get('messenger.transport.app_commands_notifier'));
        self::assertInstanceOf(InboxTransport::class, $container->get('messenger.transport.app_commands'));
        self::assertInstanceOf(InboxTransport::class, $container->get('messenger.transport.app_events'));
        self::assertInstanceOf(TransportInterface::class, $container->get('messenger.transport.app_commands_failures'));
        self::assertInstanceOf(TransportInterface::class, $container->get('messenger.transport.app_events_failures'));
    }

    public function testBrokerTransportsUseTheBundleAmqpTransport(): void
    {
        // Proves ConfigureTransportsPass swapped the jwage transport factory for the bundle's
        $container = self::getContainer();

        self::assertInstanceOf(AmqpTransport::class, $container->get('messenger.transport.commands'));
        self::assertInstanceOf(AmqpTransport::class, $container->get('messenger.transport.queries'));
        self::assertInstanceOf(AmqpTransport::class, $container->get('messenger.transport.events'));
    }

    public function testSupervisorConfigCommandIsRegistered(): void
    {
        self::bootKernel();
        $application = new Application(self::$kernel);

        $command = $application->find('messenger:supervisor-config');

        self::assertSame('messenger:supervisor-config', $command->getName());
    }
}
