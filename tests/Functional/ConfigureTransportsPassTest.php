<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Functional;

use Jwage\PhpAmqpLibMessengerBundle\Transport\AmqpTransportFactory as JwageAmqpTransportFactory;
use Jwage\PhpAmqpLibMessengerBundle\Transport\ConnectionFactory;
use Kraz\MessengerWorkflow\DependencyInjection\Compiler\ConfigureTransportsPass;
use Kraz\MessengerWorkflow\Messenger\Amqp\AmqpTransportFactory;
use Kraz\MessengerWorkflow\Tests\Fixture\Handler\ContractEventFromTransportHandler;
use Kraz\MessengerWorkflow\Tests\Fixture\Handler\FromTransportEventHandler;
use PHPUnit\Framework\TestCase;
use Symfony\Component\DependencyInjection\Argument\IteratorArgument;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\DependencyInjection\Definition;
use Symfony\Component\DependencyInjection\Reference;

/**
 * Spec: RabbitMQ integration — the bundle swaps in its own AMQP transport factory and
 * computes queue binding keys (direct exchange for commands, topic exchange for events).
 * Event binding keys are also derived automatically from the message classes handled by
 * the messenger handlers bound to a queue's transport (from_transport).
 */
final class ConfigureTransportsPassTest extends TestCase
{
    private function buildContainer(array $workflowTransportsConfig): ContainerBuilder
    {
        $container = new ContainerBuilder();
        $container->setParameter('messenger_workflow.messenger.transports', $workflowTransportsConfig);

        $jwageFactory = new Definition(JwageAmqpTransportFactory::class);
        $jwageFactory->setArguments([new Definition(ConnectionFactory::class)]);
        $container->setDefinition(JwageAmqpTransportFactory::class, $jwageFactory);

        $transports = [
            'events' => ['exchange' => ['name' => 'events', 'type' => 'topic']],
            'commands' => ['exchange' => ['name' => 'commands', 'type' => 'direct']],
            'queries' => ['exchange' => ['name' => 'queries', 'type' => 'direct']],
        ];
        foreach ($transports as $name => $options) {
            $definition = new Definition(\stdClass::class);
            $definition->setArguments(['phpamqplib://guest:guest@localhost', $options]);
            $container->setDefinition('messenger.transport.'.$name, $definition);
        }

        return $container;
    }

    public function testJwageTransportFactoryIsReplacedByTheBundleFactory(): void
    {
        $container = $this->buildContainer([]);

        (new ConfigureTransportsPass())->process($container);

        self::assertFalse($container->hasDefinition(JwageAmqpTransportFactory::class));
        self::assertTrue($container->hasDefinition(AmqpTransportFactory::class));

        $definition = $container->getDefinition(AmqpTransportFactory::class);
        self::assertTrue($definition->hasTag('messenger.transport_factory'));
        self::assertEquals(new Reference('messenger_workflow.result_storage'), $definition->getArgument('$resultStorage'));
    }

    public function testTopicQueueBindingKeysForEvents(): void
    {
        $container = $this->buildContainer([
            'events' => [
                'queue_bindings' => [
                    'app_events' => [
                        'owner' => 'TestApp',
                        'binding_keys' => ['Contracts\Demo\Event\SomethingHappened'],
                    ],
                ],
            ],
        ]);

        (new ConfigureTransportsPass())->process($container);

        $options = $container->getDefinition('messenger.transport.events')->getArgument(1);

        self::assertSame(
            [
                'events.internal.TestApp.#',
                'events.Demo.Event.SomethingHappened',
            ],
            array_values($options['queues']['app_events']['binding_keys']),
        );
    }

    public function testDirectQueueBindingKeysForCommands(): void
    {
        $container = $this->buildContainer([
            'commands' => [
                'queue_bindings' => [
                    'app_commands' => ['owner' => 'TestApp'],
                ],
            ],
        ]);

        (new ConfigureTransportsPass())->process($container);

        $options = $container->getDefinition('messenger.transport.commands')->getArgument(1);

        self::assertSame(
            [
                'commands.TestApp',
                'commands.internal.TestApp',
            ],
            array_values($options['queues']['app_commands']['binding_keys']),
        );
    }

    private function registerMessageHandler(ContainerBuilder $container, string $class, array $tagAttributes, string $serviceId = ''): void
    {
        $definition = new Definition($class);
        $definition->addTag('messenger.message_handler', $tagAttributes);
        $container->setDefinition($serviceId ?: $class, $definition);
    }

    public function testEventHandlerBindingKeysAreAddedAutomatically(): void
    {
        $container = $this->buildContainer([
            'events' => [
                'queue_bindings' => [
                    'app_events' => ['owner' => 'TestApp'],
                ],
            ],
        ]);
        $this->registerMessageHandler($container, ContractEventFromTransportHandler::class, ['bus' => 'event.bus', 'from_transport' => 'app_events']);

        (new ConfigureTransportsPass())->process($container);

        $options = $container->getDefinition('messenger.transport.events')->getArgument(1);

        self::assertSame(
            [
                'events.internal.TestApp.#',
                'events.Demo.Event.SomethingHappened',
            ],
            $options['queues']['app_events']['binding_keys'],
        );
    }

    public function testAutomaticBindingKeysDoNotDuplicateConfiguredOnes(): void
    {
        $container = $this->buildContainer([
            'events' => [
                'queue_bindings' => [
                    'app_events' => [
                        'owner' => 'TestApp',
                        'binding_keys' => ['Contracts\Demo\Event\SomethingHappened'],
                    ],
                ],
            ],
        ]);
        $this->registerMessageHandler($container, ContractEventFromTransportHandler::class, ['bus' => 'event.bus', 'from_transport' => 'app_events']);

        (new ConfigureTransportsPass())->process($container);

        $options = $container->getDefinition('messenger.transport.events')->getArgument(1);

        self::assertSame(
            [
                'events.internal.TestApp.#',
                'events.Demo.Event.SomethingHappened',
            ],
            $options['queues']['app_events']['binding_keys'],
        );
    }

    public function testHandledClassIsResolvedFromTheTaggedMethod(): void
    {
        $container = $this->buildContainer([
            'events' => [
                'queue_bindings' => [
                    'app_events' => ['owner' => 'TestApp'],
                ],
            ],
        ]);
        $this->registerMessageHandler($container, FromTransportEventHandler::class, ['bus' => 'event.bus', 'from_transport' => 'app_events', 'method' => 'onEvent']);

        (new ConfigureTransportsPass())->process($container);

        $options = $container->getDefinition('messenger.transport.events')->getArgument(1);

        self::assertSame(
            [
                'events.internal.TestApp.#',
                'events.internal.Kraz.MessengerWorkflow.Tests.Fixture.Message.TransportScopedEvent',
            ],
            $options['queues']['app_events']['binding_keys'],
        );
    }

    public function testHandlesTagAttributeTakesPrecedenceOverReflection(): void
    {
        $container = $this->buildContainer([
            'events' => [
                'queue_bindings' => [
                    'app_events' => ['owner' => 'TestApp'],
                ],
            ],
        ]);
        $this->registerMessageHandler($container, \stdClass::class, ['bus' => 'event.bus', 'from_transport' => 'app_events', 'handles' => 'Contracts\Demo\Event\SomethingHappened'], 'app.some_handler');

        (new ConfigureTransportsPass())->process($container);

        $options = $container->getDefinition('messenger.transport.events')->getArgument(1);

        self::assertSame(
            [
                'events.internal.TestApp.#',
                'events.Demo.Event.SomethingHappened',
            ],
            $options['queues']['app_events']['binding_keys'],
        );
    }

    public function testOwnContextEventsAreAlreadyCoveredByTheOwnerWildcard(): void
    {
        $container = $this->buildContainer([
            'events' => [
                'queue_bindings' => [
                    // The fixture event lives under the "Kraz" root namespace, so its routing
                    // key falls under this owner's internal wildcard and needs no own binding.
                    'app_events' => ['owner' => 'Kraz'],
                ],
            ],
        ]);
        $this->registerMessageHandler($container, FromTransportEventHandler::class, ['bus' => 'event.bus', 'from_transport' => 'app_events', 'method' => 'onEvent']);

        (new ConfigureTransportsPass())->process($container);

        $options = $container->getDefinition('messenger.transport.events')->getArgument(1);

        self::assertSame(['events.internal.Kraz.#'], $options['queues']['app_events']['binding_keys']);
    }

    public function testHandlersBoundToOtherTransportsAreIgnored(): void
    {
        $container = $this->buildContainer([
            'events' => [
                'queue_bindings' => [
                    'app_events' => ['owner' => 'TestApp'],
                ],
            ],
        ]);
        $this->registerMessageHandler($container, ContractEventFromTransportHandler::class, ['bus' => 'event.bus', 'from_transport' => 'other_events']);

        (new ConfigureTransportsPass())->process($container);

        $options = $container->getDefinition('messenger.transport.events')->getArgument(1);

        self::assertSame(['events.internal.TestApp.#'], $options['queues']['app_events']['binding_keys']);
    }

    public function testCommandTransportsDoNotGetAutomaticBindingKeys(): void
    {
        $container = $this->buildContainer([
            'commands' => [
                'queue_bindings' => [
                    'app_commands' => ['owner' => 'TestApp'],
                ],
            ],
        ]);
        $this->registerMessageHandler($container, ContractEventFromTransportHandler::class, ['bus' => 'command.bus', 'from_transport' => 'app_commands']);

        (new ConfigureTransportsPass())->process($container);

        $options = $container->getDefinition('messenger.transport.commands')->getArgument(1);

        self::assertSame(
            [
                'commands.TestApp',
                'commands.internal.TestApp',
            ],
            $options['queues']['app_commands']['binding_keys'],
        );
    }

    public function testQueueBindingWithoutOwnerIsRejected(): void
    {
        $container = $this->buildContainer([
            'commands' => [
                'queue_bindings' => [
                    'app_commands' => [],
                ],
            ],
        ]);

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessageMatches('/owner of queue "app_commands" is not set/');

        (new ConfigureTransportsPass())->process($container);
    }

    public function testRejectRedeliveredMiddlewareIsRemovedFromInternalBuses(): void
    {
        $container = $this->buildContainer([]);

        foreach (['event.bus', 'inbox.bus', 'outbox.bus', 'command.bus'] as $busId) {
            $bus = new Definition(\stdClass::class);
            $bus->setArguments([new IteratorArgument([
                new Reference('messenger.middleware.reject_redelivered_message_middleware'),
                new Reference('messenger.middleware.some_other'),
            ])]);
            $container->setDefinition($busId, $bus);
        }

        (new ConfigureTransportsPass())->process($container);

        foreach (['event.bus', 'inbox.bus', 'outbox.bus'] as $busId) {
            $middleware = array_map(
                strval(...),
                $container->getDefinition($busId)->getArgument(0)->getValues(),
            );
            self::assertNotContains('messenger.middleware.reject_redelivered_message_middleware', $middleware, $busId);
            self::assertContains('messenger.middleware.some_other', $middleware, $busId);
        }

        // command.bus keeps the default middleware stack untouched
        $commandBusMiddleware = array_map(
            strval(...),
            $container->getDefinition('command.bus')->getArgument(0)->getValues(),
        );
        self::assertContains('messenger.middleware.reject_redelivered_message_middleware', $commandBusMiddleware);
    }
}
