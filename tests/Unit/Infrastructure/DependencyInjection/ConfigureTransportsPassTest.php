<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Unit\Infrastructure\DependencyInjection;

use Kraz\MessengerWorkflow\Infrastructure\DependencyInjection\Compiler\ConfigureTransportsPass;
use Kraz\MessengerWorkflow\Tests\Fixture\Handler\ContractEventFromTransportHandler;
use Kraz\MessengerWorkflow\Tests\Fixture\Handler\TestEventHandlers;
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
                    'app_custom' => ['dsn' => 'commands-inbox://default', 'retry_strategy' => ['max_retries' => 5]],
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
}
