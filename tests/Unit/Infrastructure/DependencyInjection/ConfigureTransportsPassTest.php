<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Unit\Infrastructure\DependencyInjection;

use Contracts\Demo\Command\DoSomethingCommand;
use Contracts\Demo\Command\PlanSomethingCommand;
use Contracts\Demo\Command\ReplanSomethingCommand;
use Contracts\Demo\Query\GetSomethingQuery;
use Kraz\MessengerWorkflow\Infrastructure\DependencyInjection\Compiler\ConfigureTransportsPass;
use Kraz\MessengerWorkflow\Tests\Fixture\Handler\AttributeRoutedCommandHandler;
use Kraz\MessengerWorkflow\Tests\Fixture\Handler\ContractEventFromTransportHandler;
use Kraz\MessengerWorkflow\Tests\Fixture\Handler\PlanSomethingCommandHandler;
use Kraz\MessengerWorkflow\Tests\Fixture\Handler\TestEventHandlers;
use Kraz\MessengerWorkflow\Tests\Fixture\Message\Routing\AttributeRoutedCommand;
use Kraz\MessengerWorkflow\Tests\Fixture\Message\Routing\RoutedCommandInterface;
use Kraz\MessengerWorkflow\Tests\Fixture\Message\TestCommand;
use PHPUnit\Framework\TestCase;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\DependencyInjection\Definition;
use Symfony\Component\DependencyInjection\Reference;

/**
 * Spec: RabbitMQ integration — queue binding keys are computed per queue (direct exchange
 * for commands/queries, topic exchange for events). Event binding keys are also derived
 * from the message classes handled by handlers bound to the queue via from_transport.
 * Ported from the original package for behavioral parity.
 */
final class ConfigureTransportsPassTest extends TestCase
{
    /**
     * @param array<string, mixed> $workflowTransportsConfig
     */
    private function buildContainer(array $workflowTransportsConfig): ContainerBuilder
    {
        $container = new ContainerBuilder();
        $container->setParameter('messenger_workflow.messenger.transports', $workflowTransportsConfig);

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

    private function queueBindingKeys(ContainerBuilder $container, string $transport, string $queue): mixed
    {
        $options = $container->getDefinition('messenger.transport.'.$transport)->getArgument(1);
        self::assertIsArray($options);

        $queues = $options['queues'] ?? null;
        self::assertIsArray($queues);
        $queueOptions = $queues[$queue] ?? null;
        self::assertIsArray($queueOptions);

        return $queueOptions['binding_keys'] ?? null;
    }

    /**
     * @param array<string, mixed> $tagAttributes
     */
    private function registerMessageHandler(ContainerBuilder $container, string $class, array $tagAttributes, string $serviceId = ''): void
    {
        $definition = new Definition($class);
        $definition->addTag('messenger.message_handler', $tagAttributes);
        $container->setDefinition('' !== $serviceId ? $serviceId : $class, $definition);
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

        new ConfigureTransportsPass()->process($container);

        $bindingKeys = $this->queueBindingKeys($container, 'events', 'app_events');

        self::assertSame(
            [
                'events.internal.TestApp.#',
                'events.Demo.Event.SomethingHappened',
            ],
            $bindingKeys,
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

        new ConfigureTransportsPass()->process($container);

        $bindingKeys = $this->queueBindingKeys($container, 'commands', 'app_commands');

        self::assertSame(
            [
                'commands.TestApp',
                'commands.internal.TestApp',
            ],
            $bindingKeys,
        );
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

        new ConfigureTransportsPass()->process($container);

        $bindingKeys = $this->queueBindingKeys($container, 'events', 'app_events');

        self::assertSame(
            [
                'events.internal.TestApp.#',
                'events.Demo.Event.SomethingHappened',
            ],
            $bindingKeys,
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

        new ConfigureTransportsPass()->process($container);

        $bindingKeys = $this->queueBindingKeys($container, 'events', 'app_events');

        self::assertSame(
            [
                'events.internal.TestApp.#',
                'events.Demo.Event.SomethingHappened',
            ],
            $bindingKeys,
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
        $this->registerMessageHandler($container, TestEventHandlers::class, ['bus' => 'event.bus', 'from_transport' => 'app_events', 'method' => 'first']);

        new ConfigureTransportsPass()->process($container);

        $bindingKeys = $this->queueBindingKeys($container, 'events', 'app_events');

        self::assertSame(
            [
                'events.internal.TestApp.#',
                'events.internal.Kraz.MessengerWorkflow.Tests.Fixture.Message.TestEvent',
            ],
            $bindingKeys,
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

        new ConfigureTransportsPass()->process($container);

        $bindingKeys = $this->queueBindingKeys($container, 'events', 'app_events');

        self::assertSame(
            [
                'events.internal.TestApp.#',
                'events.Demo.Event.SomethingHappened',
            ],
            $bindingKeys,
        );
    }

    public function testOwnContextEventsAreAlreadyCoveredByTheOwnerWildcard(): void
    {
        // A handler for an internal Kraz\... event on a queue owned by "Kraz": its derived
        // key is covered by the owner wildcard events.internal.Kraz.# and must be dropped.
        $container = $this->buildContainer([
            'events' => [
                'queue_bindings' => [
                    'app_events' => ['owner' => 'Kraz'],
                ],
            ],
        ]);
        $this->registerMessageHandler($container, TestEventHandlers::class, ['bus' => 'event.bus', 'from_transport' => 'app_events', 'method' => 'first']);

        new ConfigureTransportsPass()->process($container);

        $bindingKeys = $this->queueBindingKeys($container, 'events', 'app_events');

        self::assertSame(
            ['events.internal.Kraz.#'],
            $bindingKeys,
        );
    }

    public function testOutboxTransportsDefaultToZeroRetries(): void
    {
        // A Symfony retry would re-send to the outbox (new row) while reject keeps the
        // original row — duplicating the message. Outbox transports default to 0 retries.
        $container = $this->buildContainer([]);
        $container->prependExtensionConfig('framework', [
            'messenger' => [
                'transports' => [
                    'app_outbox' => 'events-outbox://default',
                    'app_notifier' => ['dsn' => 'commands-outbox://default'],
                    'app_custom' => ['dsn' => 'outbox://default', 'retry_strategy' => ['max_retries' => 5]],
                ],
            ],
        ]);
        foreach (['app_outbox', 'app_notifier', 'app_custom'] as $name) {
            $retryDefinition = new Definition(\stdClass::class);
            $retryDefinition->setArguments([3, 1000, 2, 0, 0.1]);
            $container->setDefinition('messenger.retry.multiplier_retry_strategy.'.$name, $retryDefinition);
        }

        new ConfigureTransportsPass()->process($container);

        self::assertSame(0, $container->getDefinition('messenger.retry.multiplier_retry_strategy.app_outbox')->getArgument(0));
        self::assertSame(0, $container->getDefinition('messenger.retry.multiplier_retry_strategy.app_notifier')->getArgument(0));
        self::assertSame(3, $container->getDefinition('messenger.retry.multiplier_retry_strategy.app_custom')->getArgument(0), 'Explicit max_retries must be kept');
    }

    public function testInboxTransportsGetTheWorkflowRetryStrategies(): void
    {
        $container = $this->buildContainer([]);
        $container->prependExtensionConfig('framework', [
            'messenger' => [
                'transports' => [
                    'app_commands' => 'commands-inbox://default',
                    'app_events' => 'events-inbox://default',
                    // Own table: a second commands inbox on the connection must not share zz_commands_inbox.
                    'app_custom' => ['dsn' => 'commands-inbox://default?table_name=zz_custom_inbox', 'retry_strategy' => ['max_retries' => 5]],
                    'app_outbox' => 'events-outbox://default',
                ],
            ],
        ]);
        $container->setDefinition('messenger_workflow.command_retry_strategy', new Definition(\stdClass::class));
        $container->setDefinition('messenger_workflow.event_retry_strategy', new Definition(\stdClass::class));
        $references = [];
        foreach (['app_commands', 'app_events', 'app_custom', 'app_outbox'] as $name) {
            $references[$name] = new Reference('messenger.retry.multiplier_retry_strategy.'.$name);
        }
        $locator = new Definition(\stdClass::class);
        $locator->setArguments([$references]);
        $container->setDefinition('messenger.retry_strategy_locator', $locator);

        new ConfigureTransportsPass()->process($container);

        $updated = $container->getDefinition('messenger.retry_strategy_locator')->getArgument(0);
        self::assertIsArray($updated);
        self::assertEquals(new Reference('messenger_workflow.command_retry_strategy'), $updated['app_commands']);
        self::assertEquals(new Reference('messenger_workflow.event_retry_strategy'), $updated['app_events']);
        self::assertEquals(new Reference('messenger.retry.multiplier_retry_strategy.app_custom'), $updated['app_custom'], 'An explicit retry_strategy keeps control with the application');
        self::assertEquals(new Reference('messenger.retry.multiplier_retry_strategy.app_outbox'), $updated['app_outbox'], 'Outbox transports keep max_retries=0, not a deciding strategy');
    }

    public function testWithoutInboxesTheBrokerTransportsGetTheFlowRetryStrategies(): void
    {
        // No-inbox mode: consuming the broker transport IS handler execution, so the
        // flow retry policies move onto the broker transports themselves.
        $container = $this->buildContainer([]);
        $container->prependExtensionConfig('framework', [
            'messenger' => [
                'transports' => [
                    'commands' => 'phpamqplib://guest:guest@localhost',
                    'events' => 'phpamqplib://guest:guest@localhost',
                    'app_outbox' => 'events-outbox://default',
                ],
            ],
        ]);
        $container->setDefinition('messenger_workflow.command_retry_strategy', new Definition(\stdClass::class));
        $container->setDefinition('messenger_workflow.event_retry_strategy', new Definition(\stdClass::class));
        $container->setDefinition('messenger_workflow.query_retry_strategy', new Definition(\stdClass::class));
        $references = [];
        foreach (['commands', 'events', 'queries', 'app_outbox'] as $name) {
            $references[$name] = new Reference('messenger.retry.multiplier_retry_strategy.'.$name);
        }
        $locator = new Definition(\stdClass::class);
        $locator->setArguments([$references]);
        $container->setDefinition('messenger.retry_strategy_locator', $locator);

        new ConfigureTransportsPass()->process($container);

        $updated = $container->getDefinition('messenger.retry_strategy_locator')->getArgument(0);
        self::assertIsArray($updated);
        self::assertEquals(new Reference('messenger_workflow.command_retry_strategy'), $updated['commands']);
        self::assertEquals(new Reference('messenger_workflow.event_retry_strategy'), $updated['events']);
        self::assertEquals(new Reference('messenger_workflow.query_retry_strategy'), $updated['queries'], 'Queries have no inbox segment — always wired onto the broker transport');
        self::assertEquals(new Reference('messenger.retry.multiplier_retry_strategy.app_outbox'), $updated['app_outbox']);
    }

    public function testWithInboxesTheBrokerTransportsKeepTheirStockRetryStrategies(): void
    {
        // With inboxes the broker transports only feed the receiver relay.
        $container = $this->buildContainer([]);
        $container->prependExtensionConfig('framework', [
            'messenger' => [
                'transports' => [
                    'commands' => 'phpamqplib://guest:guest@localhost',
                    'events' => 'phpamqplib://guest:guest@localhost',
                    'app_commands' => 'commands-inbox://default',
                    'app_events' => 'events-inbox://default',
                ],
            ],
        ]);
        $container->setDefinition('messenger_workflow.command_retry_strategy', new Definition(\stdClass::class));
        $container->setDefinition('messenger_workflow.event_retry_strategy', new Definition(\stdClass::class));
        $references = [];
        foreach (['commands', 'events', 'app_commands', 'app_events'] as $name) {
            $references[$name] = new Reference('messenger.retry.multiplier_retry_strategy.'.$name);
        }
        $locator = new Definition(\stdClass::class);
        $locator->setArguments([$references]);
        $container->setDefinition('messenger.retry_strategy_locator', $locator);

        new ConfigureTransportsPass()->process($container);

        $updated = $container->getDefinition('messenger.retry_strategy_locator')->getArgument(0);
        self::assertIsArray($updated);
        self::assertEquals(new Reference('messenger.retry.multiplier_retry_strategy.commands'), $updated['commands']);
        self::assertEquals(new Reference('messenger.retry.multiplier_retry_strategy.events'), $updated['events']);
        self::assertEquals(new Reference('messenger_workflow.command_retry_strategy'), $updated['app_commands']);
        self::assertEquals(new Reference('messenger_workflow.event_retry_strategy'), $updated['app_events']);
    }

    public function testStrictOrderWithMultipleConsumersInTheDsnFailsAtBoot(): void
    {
        $container = $this->buildContainer([]);
        $container->prependExtensionConfig('framework', [
            'messenger' => [
                'transports' => [
                    'app_events' => 'events-inbox://default?strict_order=true&multiple_consumers=true',
                ],
            ],
        ]);

        $this->expectException(\LogicException::class);
        $this->expectExceptionMessageMatches('/"app_events".*mutually exclusive/');

        new ConfigureTransportsPass()->process($container);
    }

    public function testStrictOrderWithMultipleConsumersInTheOptionsFailsAtBoot(): void
    {
        $container = $this->buildContainer([]);
        $container->prependExtensionConfig('framework', [
            'messenger' => [
                'transports' => [
                    'app_commands' => [
                        'dsn' => 'commands-inbox://default?strict_order=1',
                        'options' => ['multiple_consumers' => true],
                    ],
                ],
            ],
        ]);

        $this->expectException(\LogicException::class);
        $this->expectExceptionMessageMatches('/"app_commands".*mutually exclusive/');

        new ConfigureTransportsPass()->process($container);
    }

    public function testStrictOrderAloneIsAcceptedAtBoot(): void
    {
        $container = $this->buildContainer([]);
        $container->prependExtensionConfig('framework', [
            'messenger' => [
                'transports' => [
                    'app_events' => 'events-inbox://default?strict_order=true',
                    'app_commands' => 'commands-inbox://default?strict_order=true',
                ],
            ],
        ]);

        $this->expectNotToPerformAssertions();

        new ConfigureTransportsPass()->process($container);
    }

    public function testMissingOwnerThrows(): void
    {
        $container = $this->buildContainer([
            'commands' => [
                'queue_bindings' => [
                    'app_commands' => ['binding_keys' => ['foo']],
                ],
            ],
        ]);

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessageMatches('/The owner of queue "app_commands" is not set/');

        new ConfigureTransportsPass()->process($container);
    }

    public function testUnsupportedExchangeTypeThrows(): void
    {
        $container = new ContainerBuilder();
        $container->setParameter('messenger_workflow.messenger.transports', [
            'events' => ['queue_bindings' => ['app_events' => ['owner' => 'TestApp']]],
        ]);
        $definition = new Definition(\stdClass::class);
        $definition->setArguments(['phpamqplib://guest:guest@localhost', ['exchange' => ['name' => 'events', 'type' => 'fanout']]]);
        $container->setDefinition('messenger.transport.events', $definition);

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessageMatches('/The exchange type must be "direct" or "topic"/');

        new ConfigureTransportsPass()->process($container);
    }

    // --- Routes: dedicated queues for selected message classes -------------------------

    private const array WAREHOUSE_TRANSPORTS = [
        'warehouse_commands' => 'commands-inbox://warehouse',
        'warehouse_commands_notifier' => 'commands-outbox://warehouse?table_name=zz_commands_notifier',
        'warehouse_planning' => 'commands-inbox://warehouse?strict_order=true',
    ];

    /**
     * @param array<string, mixed> $workflowTransportsConfig
     * @param array<string, mixed> $frameworkTransports
     */
    private function buildRoutedContainer(array $workflowTransportsConfig, array $frameworkTransports = self::WAREHOUSE_TRANSPORTS): ContainerBuilder
    {
        $container = $this->buildContainer($workflowTransportsConfig);
        $container->prependExtensionConfig('framework', ['messenger' => ['transports' => $frameworkTransports]]);
        foreach ($frameworkTransports as $name => $transport) {
            $dsn = \is_string($transport) ? $transport : (\is_array($transport) && \is_string($transport['dsn'] ?? null) ? $transport['dsn'] : '');
            $options = \is_array($transport) && \is_array($transport['options'] ?? null) ? $transport['options'] : [];
            $definition = new Definition(\stdClass::class);
            $definition->setArguments([$dsn, $options + ['transport_name' => $name], null]);
            $container->setDefinition('messenger.transport.'.$name, $definition);
        }
        $container->setDefinition('messenger_workflow.message_route_resolver', new Definition(\stdClass::class, ['$routes' => []]));
        $container->setDefinition('messenger_workflow.command_notifier_middleware', new Definition(\stdClass::class, ['$notifierByQueue' => []]));

        return $container;
    }

    /**
     * @param array<string, mixed> $planningBinding
     *
     * @return array<string, mixed>
     */
    private function warehouseBindings(array $planningBinding = []): array
    {
        return [
            'commands' => [
                'queue_bindings' => [
                    'warehouse_commands' => ['owner' => 'Demo'],
                    'warehouse_planning' => $planningBinding + ['owner' => 'Demo', 'route' => 'planning', 'messages' => [PlanSomethingCommand::class]],
                ],
            ],
        ];
    }

    private function compiledRoutes(ContainerBuilder $container): mixed
    {
        return $container->getDefinition('messenger_workflow.message_route_resolver')->getArgument('$routes');
    }

    private function compiledNotifiers(ContainerBuilder $container): mixed
    {
        return $container->getDefinition('messenger_workflow.command_notifier_middleware')->getArgument('$notifierByQueue');
    }

    public function testARoutedQueueBindsToTheRouteKeysOnlyAndTheRegularQueueIsUnchanged(): void
    {
        $container = $this->buildRoutedContainer($this->warehouseBindings());

        new ConfigureTransportsPass()->process($container);

        self::assertSame(['commands.Demo.planning', 'commands.internal.Demo.planning'], $this->queueBindingKeys($container, 'commands', 'warehouse_planning'));
        self::assertSame(['commands.Demo', 'commands.internal.Demo'], $this->queueBindingKeys($container, 'commands', 'warehouse_commands'));
        self::assertSame([PlanSomethingCommand::class => 'planning'], $this->compiledRoutes($container));
    }

    public function testRouteKeysSurviveTheWildcardFilter(): void
    {
        // The plain context key does not "cover" the route key: only a ".#" wildcard
        // removes keys, and direct exchanges never carry one.
        $container = $this->buildRoutedContainer($this->warehouseBindings());

        new ConfigureTransportsPass()->process($container);

        $regular = $this->queueBindingKeys($container, 'commands', 'warehouse_commands');
        $routed = $this->queueBindingKeys($container, 'commands', 'warehouse_planning');
        self::assertIsArray($regular);
        self::assertIsArray($routed);
        foreach ($routed as $key) {
            self::assertNotContains($key, $regular, 'The two queues share no binding key');
        }
        self::assertCount(2, $routed);
    }

    public function testARoutedQueryQueueUsesTheSameMechanism(): void
    {
        $container = $this->buildRoutedContainer([
            'queries' => [
                'queue_bindings' => [
                    'warehouse_queries' => ['owner' => 'Demo'],
                    'warehouse_reports' => ['owner' => 'Demo', 'route' => 'reports', 'messages' => [GetSomethingQuery::class]],
                ],
            ],
        ], []);

        new ConfigureTransportsPass()->process($container);

        self::assertSame(['queries.Demo.reports', 'queries.internal.Demo.reports'], $this->queueBindingKeys($container, 'queries', 'warehouse_reports'));
        self::assertSame([GetSomethingQuery::class => 'reports'], $this->compiledRoutes($container));
        self::assertSame([], $this->compiledNotifiers($container), 'Queries have no notifier segment');
    }

    public function testAMessageRouteAttributeOnAHandledClassJoinsTheCompiledMap(): void
    {
        $container = $this->buildRoutedContainer($this->warehouseBindings());
        $this->registerMessageHandler($container, PlanSomethingCommandHandler::class, ['bus' => 'command.bus', 'method' => 'replan']);

        new ConfigureTransportsPass()->process($container);

        self::assertSame(
            [PlanSomethingCommand::class => 'planning', ReplanSomethingCommand::class => 'planning'],
            $this->compiledRoutes($container),
        );
    }

    public function testAnAttributeRouteWithoutAMatchingQueueFailsTheBuild(): void
    {
        // AttributeRoutedCommand (context "Kraz") declares route "slow" — no Kraz queue serves it.
        $container = $this->buildRoutedContainer($this->warehouseBindings());
        $this->registerMessageHandler($container, AttributeRoutedCommandHandler::class, ['bus' => 'command.bus']);

        $this->expectException(\LogicException::class);
        $this->expectExceptionMessageMatches('/declares #\[MessageRoute\(\'slow\'\)\], but no queue with route "slow"/');

        new ConfigureTransportsPass()->process($container);
    }

    public function testAnInterfaceListedUnderMessagesRoutesItsImplementors(): void
    {
        $container = $this->buildRoutedContainer([
            'commands' => [
                'queue_bindings' => [
                    'kraz_commands' => ['owner' => 'Kraz'],
                    'kraz_bulk' => ['owner' => 'Kraz', 'route' => 'bulk', 'messages' => [RoutedCommandInterface::class]],
                ],
            ],
        ], ['kraz_commands' => 'commands-inbox://kraz', 'kraz_bulk' => 'commands-inbox://kraz?table_name=zz_bulk']);

        new ConfigureTransportsPass()->process($container);

        self::assertSame([RoutedCommandInterface::class => 'bulk'], $this->compiledRoutes($container));
    }

    public function testARoutedInboxWithoutATableGetsADerivedDefaultTable(): void
    {
        $container = $this->buildRoutedContainer($this->warehouseBindings());

        new ConfigureTransportsPass()->process($container);

        $options = $container->getDefinition('messenger.transport.warehouse_planning')->getArgument(1);
        self::assertIsArray($options);
        self::assertSame('zz_commands_inbox_warehouse_planning', $options['table_name'] ?? null);

        $regular = $container->getDefinition('messenger.transport.warehouse_commands')->getArgument(1);
        self::assertIsArray($regular);
        self::assertArrayNotHasKey('table_name', $regular, 'The regular inbox keeps the scheme default');
    }

    public function testAnExplicitTableOnTheRoutedInboxIsKept(): void
    {
        $container = $this->buildRoutedContainer($this->warehouseBindings(), array_replace(self::WAREHOUSE_TRANSPORTS, ['warehouse_planning' => 'commands-inbox://warehouse?table_name=zz_planning']));

        new ConfigureTransportsPass()->process($container);

        $options = $container->getDefinition('messenger.transport.warehouse_planning')->getArgument(1);
        self::assertIsArray($options);
        self::assertArrayNotHasKey('table_name', $options, 'The DSN table wins; nothing is derived');
    }

    public function testTheRoutedQueueResolvesTheContextsNotifier(): void
    {
        $container = $this->buildRoutedContainer($this->warehouseBindings());

        new ConfigureTransportsPass()->process($container);

        self::assertSame(['warehouse_planning' => 'warehouse_commands_notifier'], $this->compiledNotifiers($container));
    }

    public function testTheRoutedQueuesOwnNotifierWinsOverTheContextsNotifier(): void
    {
        $container = $this->buildRoutedContainer($this->warehouseBindings(), self::WAREHOUSE_TRANSPORTS + [
            'warehouse_planning_notifier' => 'commands-outbox://warehouse?table_name=zz_planning_notifier',
        ]);

        new ConfigureTransportsPass()->process($container);

        self::assertSame(['warehouse_planning' => 'warehouse_planning_notifier'], $this->compiledNotifiers($container));
    }

    public function testAnExplicitNotifierOnTheBindingIsUsed(): void
    {
        $container = $this->buildRoutedContainer($this->warehouseBindings(['notifier' => 'warehouse_results']), self::WAREHOUSE_TRANSPORTS + [
            'warehouse_results' => 'commands-outbox://warehouse?table_name=zz_results',
        ]);

        new ConfigureTransportsPass()->process($container);

        self::assertSame(['warehouse_planning' => 'warehouse_results'], $this->compiledNotifiers($container));
    }

    public function testNotifierFalseDeclaresTheReducedFlowOnPurpose(): void
    {
        $container = $this->buildRoutedContainer($this->warehouseBindings(['notifier' => false]));

        new ConfigureTransportsPass()->process($container);

        self::assertSame(['warehouse_planning' => null], $this->compiledNotifiers($container));
    }

    public function testAContextWithoutANotifierLeavesTheRoutedQueueWithoutOne(): void
    {
        $container = $this->buildRoutedContainer($this->warehouseBindings(), [
            'warehouse_commands' => 'commands-inbox://warehouse',
            'warehouse_planning' => 'commands-inbox://warehouse',
        ]);

        new ConfigureTransportsPass()->process($container);

        self::assertSame(['warehouse_planning' => null], $this->compiledNotifiers($container));
    }

    public function testAnUnknownExplicitNotifierFailsTheBuild(): void
    {
        $container = $this->buildRoutedContainer($this->warehouseBindings(['notifier' => 'nope']));

        $this->expectException(\LogicException::class);
        $this->expectExceptionMessageMatches('/the notifier "nope" is not a configured outbox transport/');

        new ConfigureTransportsPass()->process($container);
    }

    public function testANotifierOnAnotherConnectionThanTheRoutedInboxFailsTheBuild(): void
    {
        $container = $this->buildRoutedContainer($this->warehouseBindings(), [
            'warehouse_commands' => 'commands-inbox://warehouse',
            'warehouse_commands_notifier' => 'commands-outbox://other?table_name=zz_commands_notifier',
            'warehouse_planning' => 'commands-inbox://warehouse',
        ]);

        $this->expectException(\LogicException::class);
        $this->expectExceptionMessageMatches('/must use the same DBAL connection as the inbox transport "warehouse_planning"/');

        new ConfigureTransportsPass()->process($container);
    }

    public function testAClassAssignedToTwoRoutesFailsTheBuild(): void
    {
        $container = $this->buildRoutedContainer([
            'commands' => [
                'queue_bindings' => [
                    'warehouse_commands' => ['owner' => 'Demo'],
                    'warehouse_planning' => ['owner' => 'Demo', 'route' => 'planning', 'messages' => [PlanSomethingCommand::class]],
                    'warehouse_slow' => ['owner' => 'Demo', 'route' => 'slow', 'messages' => [PlanSomethingCommand::class]],
                ],
            ],
        ], self::WAREHOUSE_TRANSPORTS + ['warehouse_slow' => 'commands-inbox://warehouse?table_name=zz_slow']);

        $this->expectException(\LogicException::class);
        $this->expectExceptionMessageMatches('/already routed to queue "warehouse_planning" \(route "planning"\)/');

        new ConfigureTransportsPass()->process($container);
    }

    public function testAClassOfAnotherContextThanTheOwnerFailsTheBuild(): void
    {
        // TestCommand lives in the "Kraz" context, the queue is owned by "Demo".
        $container = $this->buildRoutedContainer($this->warehouseBindings(['messages' => [TestCommand::class]]));

        $this->expectException(\LogicException::class);
        $this->expectExceptionMessageMatches('/belongs to another bounded context than the queue owner "Demo"/');

        new ConfigureTransportsPass()->process($container);
    }

    public function testAClassNotImplementingTheMarkerInterfaceFailsTheBuild(): void
    {
        $container = $this->buildRoutedContainer($this->warehouseBindings(['messages' => [GetSomethingQuery::class]]));

        $this->expectException(\LogicException::class);
        $this->expectExceptionMessageMatches('/must implement "Kraz\\\\MessengerWorkflow\\\\Application\\\\CommandInterface"/');

        new ConfigureTransportsPass()->process($container);
    }

    public function testAnUnknownClassFailsTheBuild(): void
    {
        $container = $this->buildRoutedContainer($this->warehouseBindings(['messages' => ['Contracts\Demo\Command\Missing']]));

        $this->expectException(\LogicException::class);
        $this->expectExceptionMessageMatches('/"Contracts\\\\Demo\\\\Command\\\\Missing" does not exist/');

        new ConfigureTransportsPass()->process($container);
    }

    public function testAConfigurationDisagreeingWithTheAttributeFailsTheBuild(): void
    {
        // ReplanSomethingCommand carries #[MessageRoute('planning')].
        $container = $this->buildRoutedContainer([
            'commands' => [
                'queue_bindings' => [
                    'warehouse_commands' => ['owner' => 'Demo'],
                    'warehouse_slow' => ['owner' => 'Demo', 'route' => 'slow', 'messages' => [ReplanSomethingCommand::class]],
                ],
            ],
        ], self::WAREHOUSE_TRANSPORTS + ['warehouse_slow' => 'commands-inbox://warehouse?table_name=zz_slow']);

        $this->expectException(\LogicException::class);
        $this->expectExceptionMessageMatches('/listed for route "slow" but declares #\[MessageRoute\(\'planning\'\)\]/');

        new ConfigureTransportsPass()->process($container);
    }

    public function testARouteWithoutARegularQueueOfTheOwnerFailsTheBuild(): void
    {
        $container = $this->buildRoutedContainer([
            'commands' => [
                'queue_bindings' => [
                    'warehouse_planning' => ['owner' => 'Demo', 'route' => 'planning', 'messages' => [PlanSomethingCommand::class]],
                ],
            ],
        ]);

        $this->expectException(\LogicException::class);
        $this->expectExceptionMessageMatches('/no queue without a route is declared for that owner/');

        new ConfigureTransportsPass()->process($container);
    }

    public function testTwoRegularQueuesOfOneOwnerOnADirectExchangeFailTheBuild(): void
    {
        $container = $this->buildRoutedContainer([
            'commands' => [
                'queue_bindings' => [
                    'warehouse_commands' => ['owner' => 'Demo'],
                    'warehouse_commands_2' => ['owner' => 'Demo'],
                ],
            ],
        ]);

        $this->expectException(\LogicException::class);
        $this->expectExceptionMessageMatches('/the owner "Demo" is already the owner of queue "warehouse_commands"/');

        new ConfigureTransportsPass()->process($container);
    }

    public function testTwoQueuesOfOneOwnerOnTheTopicExchangeAreFine(): void
    {
        // Events fan out: several queues of one context are a legitimate topology.
        $container = $this->buildContainer([
            'events' => [
                'queue_bindings' => [
                    'warehouse_events' => ['owner' => 'Warehouse'],
                    'warehouse_dashboard_events' => ['owner' => 'Warehouse'],
                ],
            ],
        ]);

        new ConfigureTransportsPass()->process($container);

        self::assertSame(['events.internal.Warehouse.#'], $this->queueBindingKeys($container, 'events', 'warehouse_dashboard_events'));
    }

    public function testARouteOnTheTopicExchangeFailsTheBuild(): void
    {
        $container = $this->buildContainer([
            'events' => [
                'queue_bindings' => [
                    'warehouse_events' => ['owner' => 'Warehouse', 'route' => 'x'],
                ],
            ],
        ]);

        $this->expectException(\LogicException::class);
        $this->expectExceptionMessageMatches('/routes are only supported on direct exchanges/');

        new ConfigureTransportsPass()->process($container);
    }

    public function testTheSameRouteTwiceForOneOwnerFailsTheBuild(): void
    {
        $container = $this->buildRoutedContainer([
            'commands' => [
                'queue_bindings' => [
                    'warehouse_commands' => ['owner' => 'Demo'],
                    'warehouse_planning' => ['owner' => 'Demo', 'route' => 'planning', 'messages' => [PlanSomethingCommand::class]],
                    'warehouse_planning_2' => ['owner' => 'Demo', 'route' => 'planning', 'messages' => [DoSomethingCommand::class]],
                ],
            ],
        ], self::WAREHOUSE_TRANSPORTS + ['warehouse_planning_2' => 'commands-inbox://warehouse?table_name=zz_p2']);

        $this->expectException(\LogicException::class);
        $this->expectExceptionMessageMatches('/the route "planning" of owner "Demo" is already served by queue "warehouse_planning"/');

        new ConfigureTransportsPass()->process($container);
    }

    public function testAnInvalidRouteNameFailsTheBuild(): void
    {
        $container = $this->buildRoutedContainer($this->warehouseBindings(['route' => 'plan.ning']));

        $this->expectException(\LogicException::class);
        $this->expectExceptionMessageMatches('/the route must be one routing-key segment/');

        new ConfigureTransportsPass()->process($container);
    }

    public function testExplicitBindingKeysOnARoutedQueueFailTheBuild(): void
    {
        $container = $this->buildRoutedContainer($this->warehouseBindings(['binding_keys' => ['commands.Other']]));

        $this->expectException(\LogicException::class);
        $this->expectExceptionMessageMatches('/"binding_keys" cannot be combined with "route"/');

        new ConfigureTransportsPass()->process($container);
    }

    public function testARouteRoutingNoClassFailsTheBuild(): void
    {
        $container = $this->buildRoutedContainer($this->warehouseBindings(['messages' => []]));

        $this->expectException(\LogicException::class);
        $this->expectExceptionMessageMatches('/the route "planning" routes no message class/');

        new ConfigureTransportsPass()->process($container);
    }

    public function testARoutedQueueWithoutAnInboxWhileTheRegularQueueHasOneFailsTheBuild(): void
    {
        $container = $this->buildRoutedContainer($this->warehouseBindings(), [
            'warehouse_commands' => 'commands-inbox://warehouse',
        ]);

        $this->expectException(\LogicException::class);
        $this->expectExceptionMessageMatches('/the routed queue has no an inbox transport|routed queue has no inbox transport named after it while the regular queue "warehouse_commands" of owner "Demo" has one/');

        new ConfigureTransportsPass()->process($container);
    }

    public function testARoutedQueueInNoInboxModeNextToANoInboxRegularQueueIsFine(): void
    {
        $container = $this->buildRoutedContainer($this->warehouseBindings(), [
            'warehouse_commands_notifier' => 'commands-outbox://warehouse',
        ]);

        new ConfigureTransportsPass()->process($container);

        self::assertSame(['commands.Demo.planning', 'commands.internal.Demo.planning'], $this->queueBindingKeys($container, 'commands', 'warehouse_planning'));
        self::assertSame(['warehouse_planning' => 'warehouse_commands_notifier'], $this->compiledNotifiers($container));
    }

    // --- Storage-table isolation ---------------------------------------------------------

    public function testTwoCommandsInboxesWithoutTablesOnOneConnectionFailTheBuild(): void
    {
        $container = $this->buildContainer([]);
        $container->prependExtensionConfig('framework', ['messenger' => ['transports' => [
            'a_commands' => 'commands-inbox://main',
            'b_commands' => ['dsn' => 'commands-inbox://main'],
        ]]]);

        $this->expectException(\LogicException::class);
        $this->expectExceptionMessageMatches('/"a_commands", "b_commands": they share the storage table "zz_commands_inbox" on the DBAL connection "main".*table_name=zz_commands_inbox_b_commands/');

        new ConfigureTransportsPass()->process($container);
    }

    public function testTwoEventsInboxesTwoCommandsOutboxesAndTwoEventsOutboxesWithoutTablesFailTheBuild(): void
    {
        foreach (['events-inbox://', 'commands-outbox://', 'events-outbox://', 'inbox://', 'outbox://'] as $scheme) {
            $container = $this->buildContainer([]);
            $container->prependExtensionConfig('framework', ['messenger' => ['transports' => [
                'first' => $scheme.'main',
                'second' => $scheme.'main',
            ]]]);

            try {
                new ConfigureTransportsPass()->process($container);
                self::fail(\sprintf('Two %s transports on one table must fail', $scheme));
            } catch (\LogicException $exception) {
                self::assertStringContainsString('"first", "second": they share the storage table', $exception->getMessage(), $scheme);
            }
        }
    }

    public function testASharedTableInTheOptionsIsDetectedToo(): void
    {
        $container = $this->buildContainer([]);
        $container->prependExtensionConfig('framework', ['messenger' => ['transports' => [
            'first' => ['dsn' => 'commands-outbox://main', 'options' => ['table_name' => 'zz_shared']],
            'second' => 'commands-outbox://main?table_name=zz_shared',
        ]]]);

        $this->expectException(\LogicException::class);
        $this->expectExceptionMessageMatches('/share the storage table "zz_shared"/');

        new ConfigureTransportsPass()->process($container);
    }

    public function testASharedExplicitIndexTableFailsTheBuild(): void
    {
        $container = $this->buildContainer([]);
        $container->prependExtensionConfig('framework', ['messenger' => ['transports' => [
            'first' => 'commands-inbox://main?index_table_name=zz_idx',
            'second' => 'commands-inbox://main?table_name=zz_other&index_table_name=zz_idx',
        ]]]);

        $this->expectException(\LogicException::class);
        $this->expectExceptionMessageMatches('/share the deduplication index table "zz_idx"/');

        new ConfigureTransportsPass()->process($container);
    }

    public function testDistinctTablesConnectionsAndSchemesPass(): void
    {
        $container = $this->buildContainer([]);
        $container->prependExtensionConfig('framework', ['messenger' => ['transports' => [
            'a_commands' => 'commands-inbox://a',
            'b_commands' => 'commands-inbox://b',                      // other connection
            'a_events' => 'events-inbox://a',                           // other scheme default
            'a_planning' => 'commands-inbox://a?table_name=zz_planning', // explicit table
            'a_outbox' => 'commands-outbox://a',
            'a_notifier' => 'commands-outbox://a?table_name=zz_commands_notifier',
            'a_env' => '%env(SOME_DSN)%',                                // unresolvable — ignored
            // Failures transports share their table by design (filtered by queue_name).
            'a_commands_failures' => 'commands-failures://a?queue_name=a_commands',
            'a_planning_failures' => 'commands-failures://a?queue_name=a_planning',
        ]]]);

        $this->expectNotToPerformAssertions();

        new ConfigureTransportsPass()->process($container);
    }
}
