<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow;

use Kraz\MessengerWorkflow\Application\Attribute\AsCommandHandler;
use Kraz\MessengerWorkflow\Application\Attribute\AsEventHandler;
use Kraz\MessengerWorkflow\Application\Attribute\AsQueryHandler;
use Kraz\MessengerWorkflow\Application\Messenger\CommandBusInterface;
use Kraz\MessengerWorkflow\Application\Messenger\EventBusInterface;
use Kraz\MessengerWorkflow\Application\Messenger\QueryBusInterface;
use Kraz\MessengerWorkflow\DependencyInjection\Compiler\ConfigureTransportsPass;
use Kraz\MessengerWorkflow\Messenger\CommandBus;
use Kraz\MessengerWorkflow\Messenger\Console\MessengerSupervisorConfigCommand;
use Kraz\MessengerWorkflow\Messenger\Doctrine\Transport\Inbox\InboxTransportFactory;
use Kraz\MessengerWorkflow\Messenger\Doctrine\Transport\Outbox\OutboxTransportFactory;
use Kraz\MessengerWorkflow\Messenger\EventBus;
use Kraz\MessengerWorkflow\Messenger\EventListener\MessageFailedEventListener;
use Kraz\MessengerWorkflow\Messenger\EventListener\PostgreSqlNotifyOnIdleListener;
use Kraz\MessengerWorkflow\Messenger\Middleware\AddMessageIdStampMiddleware;
use Kraz\MessengerWorkflow\Messenger\Middleware\CommandMiddleware;
use Kraz\MessengerWorkflow\Messenger\Middleware\EventMiddleware;
use Kraz\MessengerWorkflow\Messenger\Middleware\InboxMiddleware;
use Kraz\MessengerWorkflow\Messenger\Middleware\OutboxMiddleware;
use Kraz\MessengerWorkflow\Messenger\Middleware\QueryMiddleware;
use Kraz\MessengerWorkflow\Messenger\QueryBus;
use Kraz\MessengerWorkflow\Messenger\Transport\CommandsFailuresTransportFactory;
use Kraz\MessengerWorkflow\Messenger\Transport\CommandsInboxTransportFactory;
use Kraz\MessengerWorkflow\Messenger\Transport\CommandsOutboxTransportFactory;
use Kraz\MessengerWorkflow\Messenger\Transport\EventsFailuresTransportFactory;
use Kraz\MessengerWorkflow\Messenger\Transport\EventsInboxTransportFactory;
use Kraz\MessengerWorkflow\Messenger\Transport\EventsOutboxTransportFactory;
use Kraz\MessengerWorkflow\Messenger\Transport\InMemoryResultStorage;
use Kraz\MessengerWorkflow\Messenger\Transport\RedisResultStorage;
use Kraz\MessengerWorkflow\Messenger\Transport\ResultStorageInterface;
use Symfony\Component\Config\Definition\Builder\ArrayNodeDefinition;
use Symfony\Component\Config\Definition\Configurator\DefinitionConfigurator;
use Symfony\Component\DependencyInjection\ChildDefinition;
use Symfony\Component\DependencyInjection\Compiler\PassConfig;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\DependencyInjection\Exception\LogicException;
use Symfony\Component\DependencyInjection\Loader\Configurator\ContainerConfigurator;
use Symfony\Component\HttpKernel\Bundle\AbstractBundle;
use function Symfony\Component\DependencyInjection\Loader\Configurator\service;

final class MessengerWorkflowBundle extends AbstractBundle
{
    protected string $extensionAlias = 'messenger_workflow';

    #[\Override]
    public function configure(DefinitionConfigurator $definition): void
    {
        $messengerWorkflowWorkerConfig = function (?string $name, bool $asCollection = false) {
            $node = (new ArrayNodeDefinition($name));
            ($asCollection ? $node->arrayPrototype() : $node)
                ->beforeNormalization()
                    ->ifTrue(static fn ($v) => null !== ($v['type'] ?? null))
                    ->then(static function ($v) {
                        switch ($v['type']) {
                            case 'event_publisher':
                            case 'event_handler':
                                $v['target'] = $v['target'] ?? 'event.bus';
                                break;
                            case 'event_receiver':
                                $v['source'] = $v['source'] ?? 'events';
                                $v['target'] = $v['target'] ?? 'inbox.bus';
                                $v['cmd_extra_options']['sleep'] = $v['cmd_extra_options']['sleep'] ?? 0.0;
                                break;
                            case 'command_receiver':
                                $v['source'] = $v['source'] ?? 'commands';
                                $v['target'] = $v['target'] ?? 'inbox.bus';
                                $v['cmd_extra_options']['sleep'] = $v['cmd_extra_options']['sleep'] ?? 0.0;
                                break;
                            case 'command_handler':
                                $v['target'] = $v['target'] ?? 'command.bus';
                                break;
                            case 'command_notifier':
                                $v['target'] = $v['target'] ?? 'outbox.bus';
                                break;
                            case 'query_handler':
                                $v['source'] = $v['source'] ?? 'queries';
                                $v['target'] = $v['target'] ?? 'query.bus';
                                $v['cmd_extra_options']['sleep'] = $v['cmd_extra_options']['sleep'] ?? 0.0;
                                break;
                            default:
                                throw new \LogicException(\sprintf('Unknown worker type: "%s"', $v['type']));
                        }

                        return $v;
                    })
                ->end()
                ->validate()
                    ->ifTrue(static fn ($v) => 'event_publisher' === ($v['type'] ?? null) && !($v['source'] ?? null))
                    ->thenInvalid('The event publisher worker configuration must have a "source" transport. Context: %s')
                ->end()
                ->validate()
                    ->ifTrue(static fn ($v) => 'event_receiver' === ($v['type'] ?? null) && !($v['queue'] ?? null))
                    ->thenInvalid('The event receiver worker configuration must have a "queue" set. Context: %s')
                ->end()
                ->validate()
                    ->ifTrue(static fn ($v) => 'event_handler' === ($v['type'] ?? null) && !($v['source'] ?? null))
                    ->thenInvalid('The event handler worker configuration must have a "source" transport. Context: %s')
                ->end()
                ->validate()
                    ->ifTrue(static fn ($v) => 'command_receiver' === ($v['type'] ?? null) && !($v['queue'] ?? null))
                    ->thenInvalid('The command receiver worker configuration must have a "queue" set. Context: %s')
                ->end()
                ->validate()
                    ->ifTrue(static fn ($v) => 'command_handler' === ($v['type'] ?? null) && !($v['source'] ?? null))
                    ->thenInvalid('The command handler worker configuration must have a "source" transport. Context: %s')
                ->end()
                ->validate()
                    ->ifTrue(static fn ($v) => 'command_notifier' === ($v['type'] ?? null) && !($v['source'] ?? null))
                    ->thenInvalid('The command notifier worker configuration must have a "source" transport. Context: %s')
                ->end()
                ->validate()
                    ->ifTrue(static fn ($v) => 'query_handler' === ($v['type'] ?? null) && !($v['queue'] ?? null))
                    ->thenInvalid('The query handler worker configuration must have a "queue" set. Context: %s')
                ->end()
                ->children()
                    ->scalarNode('name')->end()
                    ->scalarNode('group')->end()
                    ->scalarNode('type')->end()
                    ->scalarNode('source')->end()
                    ->scalarNode('target')->end()
                    ->scalarNode('queue')->end()
                    ->integerNode('instances')->defaultValue(1)->end()
                    ->arrayNode('cmd_extra_options')
                        ->children()
                            ->integerNode('limit')->end()
                            ->integerNode('failure_limit')->end()
                            ->integerNode('memory_limit')->end()
                            ->integerNode('time_limit')->end()
                            ->integerNode('fetch_size')->min(1)->end()
                            ->floatNode('sleep')->end()
                            ->scalarNode('verbose')->end()
                        ->end()
                    ->end()
                    ->arrayNode('supervisor')
                        ->normalizeKeys(false)
                        ->defaultValue([])
                        ->prototype('variable')->end()
                    ->end()
                ->end();

            return $node;
        };

        $definition->rootNode()
            ->children()
                ->arrayNode('messenger')
                    ->children()
                        ->arrayNode('transports')
                            ->normalizeKeys(false)
                            ->arrayPrototype()
                                ->children()
                                    ->arrayNode('orm_mappings')
                                        ->useAttributeAsKey('queue')
                                        ->normalizeKeys(false)
                                        ->defaultValue([])
                                        ->prototype('variable')
                                        ->end()
                                    ->end()
                                    ->arrayNode('queue_bindings')
                                        ->useAttributeAsKey('queue')
                                        ->normalizeKeys(false)
                                        ->defaultValue([])
                                        ->prototype('variable')
                                        ->end()
                                    ->end()
                                ->end()
                            ->end()
                        ->end()
                        ->arrayNode('workflow')
                            ->append($messengerWorkflowWorkerConfig('worker_defaults'))
                            ->children()
                                ->append($messengerWorkflowWorkerConfig('workers', true))
                            ->end()
                        ->end()
                        ->arrayNode('result_storage')
                            ->children()
                                ->scalarNode('provider')->end()
                                ->scalarNode('service')->end()
                            ->end()
                        ->end()
                    ->end()
                ->end()
            ->end();
    }

    #[\Override]
    public function prependExtension(ContainerConfigurator $container, ContainerBuilder $builder): void
    {
        $container->import('../config/packages/*.{php,yaml}');
    }

    #[\Override]
    public function loadExtension(array $config, ContainerConfigurator $container, ContainerBuilder $builder): void
    {
        $container->parameters()->set('messenger_workflow.messenger.transports', $config['messenger']['transports'] ?? []);

        $services = $container->services();
        $services
            ->defaults()
            ->autowire()
            ->autoconfigure();

        // Add message ID stamp middleware
        $services->set('messenger_workflow.add_message_id_stamp_middleware')
            ->class(AddMessageIdStampMiddleware::class);

        // PostgreSQL LISTEN/NOTIFY idle listener: lets workers block on a notification while idle
        // instead of polling, and supports consuming from multiple inbox/outbox queues at once.
        $services->set('messenger_workflow.postgresql_notify_on_idle_listener')
            ->class(PostgreSqlNotifyOnIdleListener::class)
            ->arg('$logger', service('logger')->nullOnInvalid())
            ->arg('$clock', service('clock')->nullOnInvalid())
            ->tag('kernel.event_subscriber');

        // Event bus
        $this->registerAttributeMessengerHandler($builder, AsEventHandler::class, 'event.bus');
        $services
            ->set(EventBusInterface::class)
            ->class(EventBus::class)
            ->args([service('event.bus')]);
        $services->set('messenger_workflow.event_middleware')
            ->class(EventMiddleware::class)
            ->arg('$receiverLocator', service('messenger.receiver_locator'))
            ->arg('$queueOrmBinding', array_map(fn ($item) => $item['orm'] ?? null, $config['messenger']['transports']['events']['orm_mappings'] ?? []));
        $services->set('messenger_workflow.events_outbox_transport')
            ->class(EventsOutboxTransportFactory::class)
            ->args([service('doctrine'), service('messenger_workflow.postgresql_notify_on_idle_listener')])
            ->tag('messenger.transport_factory');
        $services->set('messenger_workflow.events_inbox_transport')
            ->class(EventsInboxTransportFactory::class)
            ->args([service('doctrine'), service('messenger_workflow.postgresql_notify_on_idle_listener')])
            ->tag('messenger.transport_factory');
        $services->set('messenger_workflow.events_failures_transport')
            ->class(EventsFailuresTransportFactory::class)
            ->args([service('doctrine')])
            ->tag('messenger.transport_factory');

        // Command bus
        $this->registerAttributeMessengerHandler($builder, AsCommandHandler::class, 'command.bus');
        $services->set(CommandBusInterface::class)
            ->class(CommandBus::class)
            ->args([service('command.bus')]);
        $services->set('messenger_workflow.command_middleware')
            ->class(CommandMiddleware::class)
            ->arg('$receiverLocator', service('messenger.receiver_locator'))
            ->arg('$handlersLocator', service('command.bus.messenger.handlers_locator'))
            ->arg('$resultStorage', service('messenger_workflow.result_storage'))
            ->arg('$queueOrmBinding', array_map(fn ($item) => $item['orm'] ?? null, $config['messenger']['transports']['commands']['orm_mappings'] ?? []));
        $services->set('messenger_workflow.commands_outbox_transport')
            ->class(CommandsOutboxTransportFactory::class)
            ->args([service('doctrine'), service('messenger_workflow.postgresql_notify_on_idle_listener')])
            ->tag('messenger.transport_factory');
        $services->set('messenger_workflow.commands_inbox_transport')
            ->class(CommandsInboxTransportFactory::class)
            ->args([service('doctrine'), service('messenger_workflow.postgresql_notify_on_idle_listener')])
            ->tag('messenger.transport_factory');
        $services->set('messenger_workflow.commands_failures_transport')
            ->class(CommandsFailuresTransportFactory::class)
            ->args([service('doctrine')])
            ->tag('messenger.transport_factory');

        // Query bus
        $this->registerAttributeMessengerHandler($builder, AsQueryHandler::class, 'query.bus');
        $services->set(QueryBusInterface::class)
            ->class(QueryBus::class)
            ->args([service('query.bus')]);
        $services->set('messenger_workflow.query_middleware')
            ->class(QueryMiddleware::class)
            ->arg('$receiverLocator', service('messenger.receiver_locator'))
            ->arg('$handlersLocator', service('query.bus.messenger.handlers_locator'))
            ->arg('$resultStorage', service('messenger_workflow.result_storage'))
            ->arg('$queueOrmBinding', array_map(fn ($item) => $item['orm'] ?? null, $config['messenger']['transports']['queries']['orm_mappings'] ?? []));

        // Outbox bus
        $services->set('messenger_workflow.outbox_transport')
            ->class(OutboxTransportFactory::class)
            ->args([service('doctrine'), service('messenger_workflow.postgresql_notify_on_idle_listener')])
            ->tag('messenger.transport_factory');
        $services->set('messenger_workflow.outbox_middleware')
            ->class(OutboxMiddleware::class)
            ->arg('$resultStorage', service('messenger_workflow.result_storage'));

        // Inbox bus
        $services->set('messenger_workflow.inbox_transport')
            ->class(InboxTransportFactory::class)
            ->args([service('doctrine'), service('messenger_workflow.postgresql_notify_on_idle_listener')])
            ->tag('messenger.transport_factory');
        $services->set('messenger_workflow.inbox_middleware')
            ->class(InboxMiddleware::class)
            ->args([service('inbox.bus')]);

        // Event listeners
        $services
            ->set('messenger_workflow.failure_event_listener', MessageFailedEventListener::class)
            ->arg('$resultStorage', service('messenger_workflow.result_storage'))
            ->tag('kernel.event_subscriber');

        // Result storage
        $messengerResultStorageProvider = $config['messenger']['result_storage']['provider'] ?? 'memory';
        $messengerResultStorageProviderService = $config['messenger']['result_storage']['service'] ?? null;
        $services->set('messenger_workflow.result_storage.memory', InMemoryResultStorage::class);
        if ('redis' === $messengerResultStorageProvider) {
            $services->set('messenger_workflow.result_storage.redis')
                ->lazy()
                ->class(RedisResultStorage::class)
                ->arg('$redis', service($messengerResultStorageProviderService ?? 'redis_client.default'));
        }
        $services->set(ResultStorageInterface::class)
            ->lazy();
        $services->alias('messenger_workflow.result_storage', 'messenger_workflow.result_storage.'.$messengerResultStorageProvider);

        // Console commands
        $services->set('messenger_workflow.console.supervisor_config')
            ->class(MessengerSupervisorConfigCommand::class)
            ->arg('$workersConfig', $config['messenger']['workflow']['workers'] ?? [])
            ->arg('$workersDefaultConfig', $config['messenger']['workflow']['worker_defaults'] ?? [])
            ->tag('console.command');
    }

    #[\Override]
    public function build(ContainerBuilder $container): void
    {
        $container->addCompilerPass(new ConfigureTransportsPass(), PassConfig::TYPE_BEFORE_OPTIMIZATION, -100);
    }

    private function registerAttributeMessengerHandler(ContainerBuilder $builder, string $attributeClass, string $bus): void
    {
        $builder->registerAttributeForAutoconfiguration($attributeClass, static function (ChildDefinition $definition, object $attribute, \ReflectionClass|\ReflectionMethod $reflector) use ($attributeClass, $bus): void {
            $tagAttributes = get_object_vars($attribute);
            $tagAttributes['bus'] = $tagAttributes['bus'] ?? $bus;
            $tagAttributes['from_transport'] = $tagAttributes['fromTransport'];
            unset($tagAttributes['fromTransport']);
            if ($reflector instanceof \ReflectionMethod) {
                if (isset($tagAttributes['method'])) {
                    throw new LogicException(\sprintf('%s attribute cannot declare a method on "%s::%s()".', $attributeClass, $reflector->class, $reflector->name));
                }
                $tagAttributes['method'] = $reflector->getName();
            }
            $definition->addTag('messenger.message_handler', $tagAttributes);
        });
    }
}
