<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Functional;

use Kraz\MessengerWorkflow\Infrastructure\Messenger\Middleware\MessageIdMiddleware;
use Kraz\MessengerWorkflow\Tests\Fixture\Message\TestCommand;
use Kraz\MessengerWorkflow\Tests\Fixture\Message\TestEvent;
use Kraz\MessengerWorkflow\Tests\Fixture\Message\TestQuery;
use Kraz\MessengerWorkflow\Tests\Support\WorkflowKernelTestCase;
use Symfony\Component\Messenger\MessageBusInterface;
use Symfony\Component\Messenger\Transport\Sender\SendersLocator;
use Symfony\Component\Messenger\Transport\Serialization\Serializer;
use Symfony\Component\Messenger\Transport\Serialization\SigningSerializer;

/**
 * Asserts the framework.messenger configuration prepended by the bundle: the three
 * workflow buses, the json symfony-serializer default, and the routing of the marker
 * interfaces to the commands/queries/events broker transports.
 */
final class MessengerConfigurationTest extends WorkflowKernelTestCase
{
    public function testTheThreeWorkflowBusesAreRegistered(): void
    {
        $container = self::getContainer();

        foreach (['command.bus', 'query.bus', 'event.bus'] as $busId) {
            self::assertInstanceOf(MessageBusInterface::class, $container->get($busId));
        }
    }

    public function testMessageIdMiddlewareIsRegistered(): void
    {
        self::assertInstanceOf(
            MessageIdMiddleware::class,
            self::getContainer()->get('messenger_workflow.message_id_middleware'),
        );
    }

    public function testDefaultTransportSerializerIsTheJsonSymfonySerializer(): void
    {
        $serializer = self::getContainer()->get('messenger.default_serializer');

        // Symfony 8.1 decorates the default serializer with the message-signing serializer.
        if ($serializer instanceof SigningSerializer) {
            $serializer = new \ReflectionProperty(SigningSerializer::class, 'inner')->getValue($serializer);
        }

        self::assertInstanceOf(Serializer::class, $serializer);
    }

    public function testMarkerInterfacesRouteToTheBrokerTransports(): void
    {
        $locator = self::getContainer()->get('messenger.senders_locator');
        self::assertInstanceOf(SendersLocator::class, $locator);

        // Inspect the compiled routing map without instantiating the AMQP transports.
        $sendersMap = new \ReflectionProperty(SendersLocator::class, 'sendersMap')->getValue($locator);
        self::assertIsArray($sendersMap);

        self::assertSame(['events'], $sendersMap['Kraz\MessengerWorkflow\Domain\DomainEventInterface'] ?? null);
        self::assertSame(['commands'], $sendersMap['Kraz\MessengerWorkflow\Application\CommandInterface'] ?? null);
        self::assertSame(['queries'], $sendersMap['Kraz\MessengerWorkflow\Application\QueryInterface'] ?? null);
    }

    public function testWorkflowTransportsConfigIsNormalizedIntoTheParameter(): void
    {
        $parameter = self::getContainer()->getParameter('messenger_workflow.messenger.transports');
        self::assertIsArray($parameter);

        self::assertSame(
            ['owner' => 'Kraz', 'binding_keys' => ['Contracts\Demo\Event\SomethingHappened']],
            self::queueBinding($parameter, 'events', 'app_events'),
        );
        self::assertSame(['owner' => 'Demo'], self::queueBinding($parameter, 'commands', 'app_commands'));
        self::assertSame(['owner' => 'Demo'], self::queueBinding($parameter, 'queries', 'app_queries'));
    }

    /**
     * @param array<array-key, mixed> $parameter
     */
    private static function queueBinding(array $parameter, string $transport, string $queue): mixed
    {
        $transportConfig = $parameter[$transport] ?? null;
        self::assertIsArray($transportConfig);
        $queueBindings = $transportConfig['queue_bindings'] ?? null;
        self::assertIsArray($queueBindings);

        return $queueBindings[$queue] ?? null;
    }

    public function testBrokerTransportServicesAreDefined(): void
    {
        $container = self::getContainer();

        foreach (['commands', 'queries', 'events'] as $transport) {
            self::assertTrue($container->has('messenger.transport.'.$transport), $transport);
        }
    }

    public function testMarkerImplementorsResolveToTheirTransports(): void
    {
        $locator = self::getContainer()->get('messenger.senders_locator');
        self::assertInstanceOf(SendersLocator::class, $locator);

        $sendersFor = static function (object $message) use ($locator): array {
            $senders = [];
            foreach ($locator->getSenders(new \Symfony\Component\Messenger\Envelope($message)) as $alias => $sender) {
                $senders[] = $alias;
            }

            return $senders;
        };

        self::assertSame(['commands'], $sendersFor(new TestCommand()));
        self::assertSame(['queries'], $sendersFor(new TestQuery()));
        self::assertSame(['events'], $sendersFor(new TestEvent()));
    }
}
