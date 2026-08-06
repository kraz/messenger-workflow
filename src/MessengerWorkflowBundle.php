<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow;

use Kraz\MessengerWorkflow\Application\Attribute\AsCommandHandler;
use Kraz\MessengerWorkflow\Application\Attribute\AsEventHandler;
use Kraz\MessengerWorkflow\Application\Attribute\AsQueryHandler;
use Kraz\MessengerWorkflow\Application\Messenger\CommandBusInterface;
use Kraz\MessengerWorkflow\Application\Messenger\EventBusInterface;
use Kraz\MessengerWorkflow\Application\Messenger\QueryBusInterface;
use Kraz\MessengerWorkflow\Application\Task\ResultStorageInterface;
use Kraz\MessengerWorkflow\Application\Task\TaskOwnerResolverInterface;
use Kraz\MessengerWorkflow\Application\Task\TaskOwnershipRegistryInterface;
use Kraz\MessengerWorkflow\Application\Task\TaskResultProviderInterface;
use Kraz\MessengerWorkflow\Application\Task\TaskStatusProviderInterface;
use Kraz\MessengerWorkflow\Domain\OutboxBusInterface;
use Kraz\MessengerWorkflow\Infrastructure\Console\MessengerSupervisorConfigCommand;
use Kraz\MessengerWorkflow\Infrastructure\DependencyInjection\Compiler\ConfigureTransportsPass;
use Kraz\MessengerWorkflow\Infrastructure\DependencyInjection\Compiler\DeriveWorkersPass;
use Kraz\MessengerWorkflow\Infrastructure\Doctrine\Failure\CommandsFailuresTransportFactory;
use Kraz\MessengerWorkflow\Infrastructure\Doctrine\Failure\EventsFailuresTransportFactory;
use Kraz\MessengerWorkflow\Infrastructure\Doctrine\Inbox\CommandsInboxTransportFactory;
use Kraz\MessengerWorkflow\Infrastructure\Doctrine\Inbox\EventsInboxTransportFactory;
use Kraz\MessengerWorkflow\Infrastructure\Doctrine\Inbox\InboxTransportFactory;
use Kraz\MessengerWorkflow\Infrastructure\Doctrine\Outbox\CommandsOutboxTransportFactory;
use Kraz\MessengerWorkflow\Infrastructure\Doctrine\Outbox\EventsOutboxTransportFactory;
use Kraz\MessengerWorkflow\Infrastructure\Doctrine\Outbox\OutboxTransportFactory;
use Kraz\MessengerWorkflow\Infrastructure\Doctrine\TransportDatabaseResolver;
use Kraz\MessengerWorkflow\Infrastructure\Doctrine\WorkflowTransportRegistry;
use Kraz\MessengerWorkflow\Infrastructure\Messenger\AmqpStampFactory;
use Kraz\MessengerWorkflow\Infrastructure\Messenger\CommandBus;
use Kraz\MessengerWorkflow\Infrastructure\Messenger\EventBus;
use Kraz\MessengerWorkflow\Infrastructure\Messenger\EventListener\MessageFailedEventListener;
use Kraz\MessengerWorkflow\Infrastructure\Messenger\EventListener\PostgreSqlNotifyOnIdleListener;
use Kraz\MessengerWorkflow\Infrastructure\Messenger\Middleware\AmqpRoutingMiddleware;
use Kraz\MessengerWorkflow\Infrastructure\Messenger\Middleware\CommandNotifierMiddleware;
use Kraz\MessengerWorkflow\Infrastructure\Messenger\Middleware\ExactlyOneHandlerMiddleware;
use Kraz\MessengerWorkflow\Infrastructure\Messenger\Middleware\InboxRelayMiddleware;
use Kraz\MessengerWorkflow\Infrastructure\Messenger\Middleware\MessageIdMiddleware;
use Kraz\MessengerWorkflow\Infrastructure\Messenger\Middleware\OutboxRelayMiddleware;
use Kraz\MessengerWorkflow\Infrastructure\Messenger\Middleware\ResultNotifierMiddleware;
use Kraz\MessengerWorkflow\Infrastructure\Messenger\Middleware\QueryResultMiddleware;
use Kraz\MessengerWorkflow\Infrastructure\Messenger\OutboxBus;
use Kraz\MessengerWorkflow\Infrastructure\Messenger\QueryBus;
use Kraz\MessengerWorkflow\Infrastructure\Messenger\Middleware\WorkflowTransactionMiddleware;
use Kraz\MessengerWorkflow\Infrastructure\Messenger\Retry\ExceptionClassRetryDecider;
use Kraz\MessengerWorkflow\Infrastructure\Messenger\Retry\RetryDeciderInterface;
use Kraz\MessengerWorkflow\Infrastructure\Messenger\Retry\RetryDecidingStrategy;
use Kraz\MessengerWorkflow\Infrastructure\Messenger\Retry\TransientErrorRetryDecider;
use Kraz\MessengerWorkflow\Infrastructure\Task\InMemoryResultStorage;
use Kraz\MessengerWorkflow\Infrastructure\Task\RedisResultStorage;
use Kraz\MessengerWorkflow\Infrastructure\Task\RedisTaskOwnershipRegistry;
use Kraz\MessengerWorkflow\Infrastructure\Task\RedisTaskResultProvider;
use Kraz\MessengerWorkflow\Infrastructure\Task\RedisTaskStatusProvider;
use Kraz\MessengerWorkflow\Infrastructure\Task\TrackingCommandBus;
use Kraz\MessengerWorkflow\Infrastructure\Task\TrackingQueryBus;
use Kraz\MessengerWorkflow\Infrastructure\Worker\WorkerTypeDefaults;
use Symfony\Component\Config\Definition\Builder\ArrayNodeDefinition;
use Symfony\Component\Config\Definition\Configurator\DefinitionConfigurator;
use Symfony\Component\DependencyInjection\ChildDefinition;
use Symfony\Component\DependencyInjection\Compiler\PassConfig;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\DependencyInjection\Loader\Configurator\ContainerConfigurator;
use Symfony\Component\DependencyInjection\Loader\Configurator\ServicesConfigurator;
use Symfony\Component\HttpKernel\Bundle\AbstractBundle;
use Symfony\Component\Messenger\Retry\MultiplierRetryStrategy;

use function Symfony\Component\DependencyInjection\Loader\Configurator\service;
use function Symfony\Component\DependencyInjection\Loader\Configurator\tagged_iterator;

class MessengerWorkflowBundle extends AbstractBundle
{
    protected string $extensionAlias = 'messenger_workflow';

    public function configure(DefinitionConfigurator $definition): void
    {
        $workerConfigNode = static function (string $name, bool $asCollection): ArrayNodeDefinition {
            $node = new ArrayNodeDefinition($name);
            $target = $asCollection ? $node->arrayPrototype() : $node;
            $target
                ->beforeNormalization()
                    ->ifTrue(static fn ($v): bool => \is_array($v) && null !== ($v['type'] ?? null))
                    ->then(static fn (array $v): array => WorkerTypeDefaults::apply($v))
                ->end()
                ->validate()
                    ->ifTrue(static fn ($v): bool => \is_array($v) && \in_array($v['type'] ?? null, ['event_publisher', 'event_handler', 'command_handler', 'command_notifier'], true) && '' === (\is_scalar($v['source'] ?? null) ? (string) $v['source'] : ''))
                    ->thenInvalid('This worker type requires a "source" transport. Context: %s')
                ->end()
                ->validate()
                    ->ifTrue(static fn ($v): bool => \is_array($v) && \in_array($v['type'] ?? null, ['event_receiver', 'command_receiver', 'query_handler'], true) && '' === (\is_scalar($v['queue'] ?? null) ? (string) $v['queue'] : ''))
                    ->thenInvalid('This worker type requires a "queue". Context: %s')
                ->end()
                ->children()
                    ->scalarNode('name')->end()
                    ->scalarNode('group')->end()
                    ->scalarNode('type')->end()
                    ->scalarNode('source')->end()
                    ->scalarNode('target')->end()
                    ->scalarNode('queue')->end()
                    ->booleanNode('enabled')->defaultTrue()->end()
                    ->integerNode('instances')->defaultValue(1)->end()
                    ->arrayNode('labels')
                        ->info('Free-form labels attached to the worker definition.')
                        ->normalizeKeys(false)
                        ->variablePrototype()->end()
                    ->end()
                    ->arrayNode('cmd_extra_options')
                        ->children()
                            ->integerNode('limit')->end()
                            ->integerNode('failure_limit')->end()
                            ->integerNode('memory_limit')->end()
                            ->integerNode('time_limit')->end()
                            ->integerNode('fetch_size')->min(1)->end()
                            ->floatNode('sleep')->end()
                            ->integerNode('keepalive')
                                ->min(1)
                                ->info('Keepalive interval in seconds (messenger:consume --keepalive): a long-running handler refreshes its in-flight marker instead of being redelivered after redeliver_timeout. Only meaningful on multiple_consumers=true sources; must stay below the transport\'s redeliver_timeout.')
                            ->end()
                            ->scalarNode('verbose')->end()
                        ->end()
                    ->end()
                    ->arrayNode('supervisor')
                        ->normalizeKeys(false)
                        ->defaultValue([])
                        ->variablePrototype()->end()
                    ->end()
                ->end();

            return $node;
        };

        $definition->rootNode()
            ->children()
                ->arrayNode('messenger')
                    ->addDefaultsIfNotSet()
                    ->children()
                        ->arrayNode('transports')
                            ->normalizeKeys(false)
                            ->useAttributeAsKey('name')
                            ->arrayPrototype()
                                ->children()
                                    ->arrayNode('orm_mappings')
                                        ->info('Maps broker queues to entity manager names (used when handling without an inbox).')
                                        ->normalizeKeys(false)
                                        ->useAttributeAsKey('queue')
                                        ->variablePrototype()->end()
                                    ->end()
                                    ->arrayNode('queue_bindings')
                                        ->info('Queues of this broker transport: each entry declares the owning bounded context and optional explicit binding keys.')
                                        ->normalizeKeys(false)
                                        ->useAttributeAsKey('queue')
                                        ->variablePrototype()->end()
                                    ->end()
                                ->end()
                            ->end()
                        ->end()
                        ->arrayNode('defaults')
                            ->addDefaultsIfNotSet()
                            ->children()
                                ->integerNode('await_timeout')
                                    ->defaultValue(300)
                                    ->min(1)
                                    ->info('Default await() timeout in seconds for command/query tasks.')
                                ->end()
                                ->arrayNode('command_retry')
                                    ->addDefaultsIfNotSet()
                                    ->info('Command retry policy: no retries by default; transient infrastructure errors are retried with exponential backoff and jitter within a small total time budget.')
                                    ->children()
                                        ->integerNode('transient_max_retries')->defaultValue(3)->min(0)->end()
                                        ->integerNode('delay')->defaultValue(1000)->min(0)->info('Initial retry delay in milliseconds.')->end()
                                        ->floatNode('multiplier')->defaultValue(2.0)->min(1.0)->end()
                                        ->integerNode('max_delay')->defaultValue(10000)->min(0)->info('Maximum delay per retry in milliseconds (0 = uncapped).')->end()
                                        ->floatNode('jitter')->defaultValue(0.1)->min(0.0)->max(1.0)->end()
                                        ->integerNode('max_total_delay')->defaultValue(30000)->min(0)->info('Total retry-time budget in milliseconds across all attempts (0 = unbounded).')->end()
                                        ->arrayNode('retryable_exceptions')
                                            ->info('Exception classes (instanceof match) that force a retry.')
                                            ->scalarPrototype()->end()
                                        ->end()
                                        ->arrayNode('non_retryable_exceptions')
                                            ->info('Exception classes (instanceof match) that force a permanent failure. Wins over retryable_exceptions.')
                                            ->scalarPrototype()->end()
                                        ->end()
                                    ->end()
                                ->end()
                                ->arrayNode('query_retry')
                                    ->addDefaultsIfNotSet()
                                    ->info('Query retry policy: aggressive fast retries on transient infrastructure errors only; no failure transport — a permanent failure is reported to the asker through the result storage and the message is dropped.')
                                    ->children()
                                        ->integerNode('transient_max_retries')->defaultValue(10)->min(0)->end()
                                        ->integerNode('delay')->defaultValue(100)->min(0)->info('Initial retry delay in milliseconds.')->end()
                                        ->floatNode('multiplier')->defaultValue(2.0)->min(1.0)->end()
                                        ->integerNode('max_delay')->defaultValue(5000)->min(0)->info('Maximum delay per retry in milliseconds (0 = uncapped).')->end()
                                        ->floatNode('jitter')->defaultValue(0.1)->min(0.0)->max(1.0)->end()
                                        ->integerNode('max_total_delay')->defaultValue(60000)->min(0)->info('Total retry-time budget in milliseconds across all attempts (0 = unbounded).')->end()
                                        ->arrayNode('retryable_exceptions')
                                            ->info('Exception classes (instanceof match) that force a retry.')
                                            ->scalarPrototype()->end()
                                        ->end()
                                        ->arrayNode('non_retryable_exceptions')
                                            ->info('Exception classes (instanceof match) that force a permanent failure. Wins over retryable_exceptions.')
                                            ->scalarPrototype()->end()
                                        ->end()
                                    ->end()
                                ->end()
                                ->arrayNode('event_retry')
                                    ->addDefaultsIfNotSet()
                                    ->info('Event retry policy: always retried (exponential backoff and jitter) within a bounded total time budget, then DLQ — a poison message cannot block an ordered queue forever.')
                                    ->children()
                                        ->integerNode('max_retries')->defaultValue(20)->min(0)->end()
                                        ->integerNode('delay')->defaultValue(1000)->min(0)->info('Initial retry delay in milliseconds.')->end()
                                        ->floatNode('multiplier')->defaultValue(2.0)->min(1.0)->end()
                                        ->integerNode('max_delay')->defaultValue(60000)->min(0)->info('Maximum delay per retry in milliseconds (0 = uncapped).')->end()
                                        ->floatNode('jitter')->defaultValue(0.1)->min(0.0)->max(1.0)->end()
                                        ->integerNode('max_total_delay')->defaultValue(15 * 60 * 1000)->min(0)->info('Total retry-time budget in milliseconds across all attempts (0 = unbounded).')->end()
                                        ->arrayNode('retryable_exceptions')
                                            ->info('Exception classes (instanceof match) that force a retry.')
                                            ->scalarPrototype()->end()
                                        ->end()
                                        ->arrayNode('non_retryable_exceptions')
                                            ->info('Exception classes (instanceof match) that force a permanent failure. Wins over retryable_exceptions.')
                                            ->scalarPrototype()->end()
                                        ->end()
                                    ->end()
                                ->end()
                            ->end()
                        ->end()
                        ->arrayNode('outbox_buses')
                            ->info('Per-context outbox buses: "<context>: <outbox transport name>" registers an OutboxBusInterface implementation "messenger_workflow.outbox_bus.<context>" publishing through that transport.')
                            ->normalizeKeys(false)
                            ->useAttributeAsKey('context')
                            ->scalarPrototype()->end()
                        ->end()
                        ->arrayNode('result_storage')
                            ->addDefaultsIfNotSet()
                            ->children()
                                ->scalarNode('provider')
                                    ->defaultValue('memory')
                                    ->info('Result storage provider: "memory" (single-process), "redis", or a custom suffix resolved as service "messenger_workflow.result_storage.<provider>".')
                                ->end()
                                ->scalarNode('service')
                                    ->defaultNull()
                                    ->info('Service id of the \Redis client used by the redis provider (default "redis_client.default").')
                                ->end()
                                ->scalarNode('namespace')
                                    ->defaultNull()
                                    ->info('Optional key namespace: results are stored as rs:<namespace>:<taskId>.')
                                ->end()
                                ->integerNode('expire_input_after')
                                    ->defaultValue(3 * 60 * 60)
                                    ->min(1)
                                    ->info('TTL in seconds applied to stored results.')
                                ->end()
                                ->integerNode('expire_after_await')
                                    ->defaultNull()
                                    ->min(1)
                                    ->info('If set, a successful await() re-expires the result after this many seconds. Off by default: awaiting never shortens the result TTL.')
                                ->end()
                            ->end()
                        ->end()
                        ->arrayNode('workflow')
                            ->addDefaultsIfNotSet()
                            ->append($workerConfigNode('worker_defaults', false))
                            ->children()
                                ->append($workerConfigNode('workers', true))
                            ->end()
                        ->end()
                        ->arrayNode('tasks')
                            ->addDefaultsIfNotSet()
                            ->children()
                                ->booleanNode('enabled')
                                    ->defaultFalse()
                                    ->info('Registers the Redis task services (ownership registry, status/result providers) and the tracking bus decorators. Requires the "redis" result_storage provider (shares its client and namespace).')
                                ->end()
                                ->integerNode('ownership_ttl')
                                    ->defaultValue(4 * 60 * 60)
                                    ->min(1)
                                    ->info('TTL in seconds of task ownership records — keep it above the result TTL so ownership outlives results.')
                                ->end()
                                ->booleanNode('debug')
                                    ->defaultNull()
                                    ->info('Expose exception class names in task errors (default: %kernel.debug%).')
                                ->end()
                            ->end()
                        ->end()
                    ->end()
                ->end()
            ->end();
    }

    public function build(ContainerBuilder $container): void
    {
        parent::build($container);

        $container->addCompilerPass(new ConfigureTransportsPass(), PassConfig::TYPE_BEFORE_OPTIMIZATION, -100);
        $container->addCompilerPass(new DeriveWorkersPass(), PassConfig::TYPE_BEFORE_OPTIMIZATION, -100);
    }

    public function prependExtension(ContainerConfigurator $container, ContainerBuilder $builder): void
    {
        $container->import(\dirname(__DIR__).'/config/packages/*.yaml');
    }

    /**
     * @param array<array-key, mixed> $config
     */
    public function loadExtension(array $config, ContainerConfigurator $container, ContainerBuilder $builder): void
    {
        $this->registerAttributeMessengerHandler($builder, AsCommandHandler::class, 'command.bus');
        $this->registerAttributeMessengerHandler($builder, AsQueryHandler::class, 'query.bus');
        $this->registerAttributeMessengerHandler($builder, AsEventHandler::class, 'event.bus');

        $services = $container->services();

        $services->set('messenger_workflow.amqp_stamp_factory')
            ->class(AmqpStampFactory::class);

        $services->set('messenger_workflow.message_id_middleware')
            ->class(MessageIdMiddleware::class);

        $services->set('messenger_workflow.amqp_routing_middleware')
            ->class(AmqpRoutingMiddleware::class)
            ->args([service('messenger_workflow.amqp_stamp_factory')]);

        $services->set('messenger_workflow.outbox_relay_middleware')
            ->class(OutboxRelayMiddleware::class)
            ->arg('$sendersLocator', service('messenger.senders_locator'))
            ->arg('$stampFactory', service('messenger_workflow.amqp_stamp_factory'));

        $services->set('messenger_workflow.inbox_relay_middleware')
            ->class(InboxRelayMiddleware::class)
            ->arg('$sendersLocator', service('messenger.senders_locator'));

        // orm_mappings (no-inbox mode): broker transport → queue → entity
        // manager/connection name, consumed by the transaction middleware when a
        // broker queue is handled directly (no inbox transport).
        $transportsConfig = \is_array($config['messenger'] ?? null) && \is_array($config['messenger']['transports'] ?? null) ? $config['messenger']['transports'] : [];
        $queueOrmBinding = [];
        foreach ($transportsConfig as $transportName => $transportConfig) {
            $ormMappings = \is_array($transportConfig) && \is_array($transportConfig['orm_mappings'] ?? null) ? $transportConfig['orm_mappings'] : [];
            foreach ($ormMappings as $queueName => $mapping) {
                $target = \is_string($mapping) ? $mapping : (\is_array($mapping) && \is_string($mapping['orm'] ?? null) ? $mapping['orm'] : null);
                if (null !== $target && '' !== $target) {
                    $queueOrmBinding[(string) $transportName][(string) $queueName] = $target;
                }
            }
        }

        $services->set('messenger_workflow.transaction_middleware')
            ->class(WorkflowTransactionMiddleware::class)
            ->arg('$transportRegistry', service('messenger_workflow.transport_registry'))
            ->arg('$queueOrmBinding', $queueOrmBinding)
            ->arg('$doctrine', service('doctrine')->nullOnInvalid());

        $services->set('messenger_workflow.command_notifier_middleware')
            ->class(CommandNotifierMiddleware::class)
            ->arg('$receiverLocator', service('messenger.receiver_locator'))
            ->arg('$transportRegistry', service('messenger_workflow.transport_registry'))
            ->arg('$resultStorage', service('messenger_workflow.result_storage'));

        $services->set('messenger_workflow.result_notifier_middleware')
            ->class(ResultNotifierMiddleware::class)
            ->arg('$resultStorage', service('messenger_workflow.result_storage'));

        $services->set('messenger_workflow.exactly_one_handler_middleware')
            ->class(ExactlyOneHandlerMiddleware::class);

        $services->set('messenger_workflow.query_result_middleware')
            ->class(QueryResultMiddleware::class)
            ->arg('$resultStorage', service('messenger_workflow.result_storage'));

        $services->set('messenger_workflow.transport_registry')
            ->class(WorkflowTransportRegistry::class);

        $services->set('messenger_workflow.transport_database_resolver')
            ->class(TransportDatabaseResolver::class)
            ->arg('$transportRegistry', service('messenger_workflow.transport_registry'));

        $services->set('messenger_workflow.failure_event_listener')
            ->class(MessageFailedEventListener::class)
            ->arg('$resultStorage', service('messenger_workflow.result_storage'))
            ->tag('kernel.event_subscriber');

        $services->set('messenger_workflow.postgresql_notify_on_idle_listener')
            ->class(PostgreSqlNotifyOnIdleListener::class)
            ->arg('$logger', service('logger')->nullOnInvalid())
            ->arg('$clock', service('clock')->nullOnInvalid())
            ->tag('kernel.event_subscriber');

        $doctrineTransportFactories = [
            'messenger_workflow.outbox_transport' => OutboxTransportFactory::class,
            'messenger_workflow.commands_outbox_transport' => CommandsOutboxTransportFactory::class,
            'messenger_workflow.events_outbox_transport' => EventsOutboxTransportFactory::class,
            'messenger_workflow.inbox_transport' => InboxTransportFactory::class,
            'messenger_workflow.commands_inbox_transport' => CommandsInboxTransportFactory::class,
            'messenger_workflow.events_inbox_transport' => EventsInboxTransportFactory::class,
        ];
        foreach ($doctrineTransportFactories as $serviceId => $factoryClass) {
            $services->set($serviceId)
                ->class($factoryClass)
                ->args([
                    // Null when DoctrineBundle is absent — the factory then fails with a
                    // clear error only if a workflow DSN is actually used.
                    service('doctrine')->nullOnInvalid(),
                    service('messenger_workflow.postgresql_notify_on_idle_listener'),
                    service('messenger_workflow.transport_registry'),
                ])
                ->tag('messenger.transport_factory');
        }

        $failureFactories = [
            'messenger_workflow.commands_failures_transport' => CommandsFailuresTransportFactory::class,
            'messenger_workflow.events_failures_transport' => EventsFailuresTransportFactory::class,
        ];
        foreach ($failureFactories as $serviceId => $factoryClass) {
            $services->set($serviceId)
                ->class($factoryClass)
                ->args([service('doctrine')->nullOnInvalid()])
                ->tag('messenger.transport_factory');
        }

        $messengerConfig = \is_array($config['messenger'] ?? null) ? $config['messenger'] : [];
        $container->parameters()->set(
            'messenger_workflow.messenger.transports',
            \is_array($messengerConfig['transports'] ?? null) ? $messengerConfig['transports'] : [],
        );
        $this->registerResultStorage(
            \is_array($messengerConfig['result_storage'] ?? null) ? $messengerConfig['result_storage'] : [],
            $container,
        );
        $this->registerBusesAndRetryPolicies(
            \is_array($messengerConfig['defaults'] ?? null) ? $messengerConfig['defaults'] : [],
            $container,
            $builder,
        );
        $this->registerOutboxBuses(
            \is_array($messengerConfig['outbox_buses'] ?? null) ? $messengerConfig['outbox_buses'] : [],
            $container,
        );
        $this->registerTaskServices(
            \is_array($messengerConfig['tasks'] ?? null) ? $messengerConfig['tasks'] : [],
            \is_array($messengerConfig['result_storage'] ?? null) ? $messengerConfig['result_storage'] : [],
            $container,
        );

        $workflowConfig = \is_array($messengerConfig['workflow'] ?? null) ? $messengerConfig['workflow'] : [];
        $container->parameters()->set(
            'messenger_workflow.workflow.workers',
            \is_array($workflowConfig['workers'] ?? null) ? $workflowConfig['workers'] : [],
        );
        $container->parameters()->set(
            'messenger_workflow.workflow.worker_defaults',
            \is_array($workflowConfig['worker_defaults'] ?? null) ? $workflowConfig['worker_defaults'] : [],
        );
        // Overwritten by DeriveWorkersPass with the derived + merged worker set.
        $container->parameters()->set('messenger_workflow.workers', []);

        $services = $container->services();
        $services->set('messenger_workflow.supervisor_config_command')
            ->class(MessengerSupervisorConfigCommand::class)
            ->arg('$workersConfig', '%messenger_workflow.workers%')
            ->arg('$workersDefaultConfig', '%messenger_workflow.workflow.worker_defaults%')
            ->arg('$params', service('parameter_bag'))
            ->tag('console.command');
    }

    /**
     * Values are normalized by the configuration tree; the guards only narrow types.
     *
     * @param array<array-key, mixed> $tasks
     * @param array<array-key, mixed> $resultStorage
     */
    private function registerTaskServices(array $tasks, array $resultStorage, ContainerConfigurator $container): void
    {
        if (true !== ($tasks['enabled'] ?? false)) {
            return;
        }

        $provider = \is_scalar($resultStorage['provider'] ?? null) ? (string) $resultStorage['provider'] : 'memory';
        if ('redis' !== $provider) {
            throw new \LogicException('messenger_workflow.messenger.tasks requires the "redis" result_storage provider (the task services share its Redis client and key namespace).');
        }

        $clientService = \is_scalar($resultStorage['service'] ?? null) ? (string) $resultStorage['service'] : 'redis_client.default';
        $namespace = \is_scalar($resultStorage['namespace'] ?? null) ? (string) $resultStorage['namespace'] : null;
        $ownershipTtl = \is_int($tasks['ownership_ttl'] ?? null) ? $tasks['ownership_ttl'] : 4 * 60 * 60;
        $debug = \is_bool($tasks['debug'] ?? null) ? $tasks['debug'] : '%kernel.debug%';

        $services = $container->services();

        $services->set('messenger_workflow.task_ownership_registry')
            ->lazy()
            ->class(RedisTaskOwnershipRegistry::class)
            ->arg('$redis', service($clientService))
            ->arg('$logger', service('logger')->nullOnInvalid())
            ->arg('$ownershipTtl', $ownershipTtl);
        $services->alias(TaskOwnershipRegistryInterface::class, 'messenger_workflow.task_ownership_registry')
            ->public();

        $services->set('messenger_workflow.task_status_provider')
            ->lazy()
            ->class(RedisTaskStatusProvider::class)
            ->arg('$redis', service($clientService))
            ->arg('$ownership', service('messenger_workflow.task_ownership_registry'))
            ->arg('$logger', service('logger')->nullOnInvalid())
            ->arg('$debug', $debug)
            ->arg('$resultNamespace', $namespace);
        $services->alias(TaskStatusProviderInterface::class, 'messenger_workflow.task_status_provider')
            ->public();

        $services->set('messenger_workflow.task_result_provider')
            ->lazy()
            ->class(RedisTaskResultProvider::class)
            ->arg('$redis', service($clientService))
            ->arg('$resultNamespace', $namespace);
        $services->alias(TaskResultProviderInterface::class, 'messenger_workflow.task_result_provider')
            ->public();

        // Tracking decorators: the resolver is an optional application service — without
        // one, every dispatch is an ownerless system task and nothing is recorded.
        $services->set('messenger_workflow.tracking_command_bus')
            ->class(TrackingCommandBus::class)
            ->decorate('messenger_workflow.command_bus')
            ->arg('$inner', service('.inner'))
            ->arg('$registry', service('messenger_workflow.task_ownership_registry'))
            ->arg('$ownerResolver', service(TaskOwnerResolverInterface::class)->nullOnInvalid());

        $services->set('messenger_workflow.tracking_query_bus')
            ->class(TrackingQueryBus::class)
            ->decorate('messenger_workflow.query_bus')
            ->arg('$inner', service('.inner'))
            ->arg('$registry', service('messenger_workflow.task_ownership_registry'))
            ->arg('$ownerResolver', service(TaskOwnerResolverInterface::class)->nullOnInvalid());
    }

    /**
     * Values are normalized by the configuration tree; the guards only narrow types.
     *
     * @param array<array-key, mixed> $defaults
     */
    private function registerBusesAndRetryPolicies(array $defaults, ContainerConfigurator $container, ContainerBuilder $builder): void
    {
        $services = $container->services();

        $awaitTimeout = \is_int($defaults['await_timeout'] ?? null) ? $defaults['await_timeout'] : 300;

        // The buses are application-facing entry points — public like the framework's
        // own bus services, so they survive compilation unreferenced.
        $services->set('messenger_workflow.command_bus')
            ->class(CommandBus::class)
            ->arg('$messageBus', service('command.bus'))
            ->arg('$resultStorage', service('messenger_workflow.result_storage'))
            ->arg('$transportRegistry', service('messenger_workflow.transport_registry'))
            ->arg('$awaitDefaultTimeout', $awaitTimeout)
            ->public();
        $services->alias(CommandBusInterface::class, 'messenger_workflow.command_bus')
            ->public();

        $services->set('messenger_workflow.event_bus')
            ->class(EventBus::class)
            ->arg('$messageBus', service('event.bus'))
            ->public();
        $services->alias(EventBusInterface::class, 'messenger_workflow.event_bus')
            ->public();

        $services->set('messenger_workflow.query_bus')
            ->class(QueryBus::class)
            ->arg('$messageBus', service('query.bus'))
            ->arg('$resultStorage', service('messenger_workflow.result_storage'))
            ->arg('$awaitDefaultTimeout', $awaitTimeout)
            ->public();
        $services->alias(QueryBusInterface::class, 'messenger_workflow.query_bus')
            ->public();

        // Application services implementing RetryDeciderInterface join every flow's
        // decision chain between the configured exception lists and the built-in
        // transient decider.
        $builder->registerForAutoconfiguration(RetryDeciderInterface::class)
            ->addTag('messenger_workflow.retry_decider');

        $services->set('messenger_workflow.retry.transient_error_decider')
            ->class(TransientErrorRetryDecider::class);

        $this->registerRetryStrategy('command', $services, \is_array($defaults['command_retry'] ?? null) ? $defaults['command_retry'] : [], [
            'max_retries_key' => 'transient_max_retries',
            'max_retries' => 3,
            'delay' => 1000,
            'multiplier' => 2.0,
            'max_delay' => 10000,
            'max_total_delay' => 30000,
            'retry_by_default' => false,
        ]);
        $this->registerRetryStrategy('query', $services, \is_array($defaults['query_retry'] ?? null) ? $defaults['query_retry'] : [], [
            'max_retries_key' => 'transient_max_retries',
            'max_retries' => 10,
            'delay' => 100,
            'multiplier' => 2.0,
            'max_delay' => 5000,
            'max_total_delay' => 60000,
            'retry_by_default' => false,
        ]);
        $this->registerRetryStrategy('event', $services, \is_array($defaults['event_retry'] ?? null) ? $defaults['event_retry'] : [], [
            'max_retries_key' => 'max_retries',
            'max_retries' => 20,
            'delay' => 1000,
            'multiplier' => 2.0,
            'max_delay' => 60000,
            'max_total_delay' => 15 * 60 * 1000,
            'retry_by_default' => true,
        ]);
    }

    /**
     * Registers "messenger_workflow.<flow>_retry_strategy" with its own decision chain:
     * the flow's configured exception lists, application-tagged deciders, the shared
     * transient decider.
     *
     * @param array<array-key, mixed>                                                                                            $retryConfig
     * @param array{max_retries_key: string, max_retries: int, delay: int, multiplier: float, max_delay: int, max_total_delay: int, retry_by_default: bool} $flowDefaults
     */
    private function registerRetryStrategy(string $flow, ServicesConfigurator $services, array $retryConfig, array $flowDefaults): void
    {
        $classList = static fn (mixed $value): array => \is_array($value) ? array_values(array_filter($value, \is_string(...))) : [];

        $services->set(\sprintf('messenger_workflow.retry.%s_exception_class_decider', $flow))
            ->class(ExceptionClassRetryDecider::class)
            ->arg('$retryableExceptions', $classList($retryConfig['retryable_exceptions'] ?? null))
            ->arg('$nonRetryableExceptions', $classList($retryConfig['non_retryable_exceptions'] ?? null));

        $services->set(\sprintf('messenger_workflow.retry.%s_delay_strategy', $flow))
            ->class(MultiplierRetryStrategy::class)
            ->args([
                \is_int($retryConfig[$flowDefaults['max_retries_key']] ?? null) ? $retryConfig[$flowDefaults['max_retries_key']] : $flowDefaults['max_retries'],
                \is_int($retryConfig['delay'] ?? null) ? $retryConfig['delay'] : $flowDefaults['delay'],
                \is_float($retryConfig['multiplier'] ?? null) ? $retryConfig['multiplier'] : $flowDefaults['multiplier'],
                \is_int($retryConfig['max_delay'] ?? null) ? $retryConfig['max_delay'] : $flowDefaults['max_delay'],
                \is_float($retryConfig['jitter'] ?? null) ? $retryConfig['jitter'] : 0.1,
            ]);

        $services->set(\sprintf('messenger_workflow.%s_retry_strategy', $flow))
            ->class(RetryDecidingStrategy::class)
            ->arg('$deciders', [
                service(\sprintf('messenger_workflow.retry.%s_exception_class_decider', $flow)),
                tagged_iterator('messenger_workflow.retry_decider'),
                service('messenger_workflow.retry.transient_error_decider'),
            ])
            ->arg('$delayStrategy', service(\sprintf('messenger_workflow.retry.%s_delay_strategy', $flow)))
            ->arg('$maxTotalDelayMs', \is_int($retryConfig['max_total_delay'] ?? null) ? $retryConfig['max_total_delay'] : $flowDefaults['max_total_delay'])
            ->arg('$retryByDefault', $flowDefaults['retry_by_default']);
    }

    /**
     * @param array<array-key, mixed> $outboxBuses
     */
    private function registerOutboxBuses(array $outboxBuses, ContainerConfigurator $container): void
    {
        $services = $container->services();
        $serviceIds = [];

        foreach ($outboxBuses as $context => $transportName) {
            if (!\is_string($transportName) || '' === $transportName) {
                continue;
            }
            $context = (string) $context;

            $serviceId = 'messenger_workflow.outbox_bus.'.$context;
            $services->set($serviceId)
                ->class(OutboxBus::class)
                ->arg('$messageBus', service('event.bus'))
                ->arg('$transportName', $transportName)
                ->public();
            // Named autowiring: OutboxBusInterface $<context>OutboxBus (snake_case contexts camelize).
            $camelContext = lcfirst(str_replace(' ', '', ucwords(str_replace(['_', '-', '.'], ' ', $context))));
            $services->alias(OutboxBusInterface::class.' $'.$camelContext.'OutboxBus', $serviceId);
            $serviceIds[] = $serviceId;
        }

        if (1 === \count($serviceIds)) {
            $services->alias(OutboxBusInterface::class, $serviceIds[0])
                ->public();
        }
    }

    /**
     * Values are normalized by the configuration tree; the guards only narrow types.
     *
     * @param array<array-key, mixed> $config
     */
    private function registerResultStorage(array $config, ContainerConfigurator $container): void
    {
        $services = $container->services();
        $provider = \is_scalar($config['provider'] ?? null) ? (string) $config['provider'] : 'memory';

        $services->set('messenger_workflow.result_storage.memory', InMemoryResultStorage::class)
            ->tag('kernel.reset', ['method' => 'reset']);

        if ('redis' === $provider) {
            $clientService = \is_scalar($config['service'] ?? null) ? (string) $config['service'] : 'redis_client.default';
            $namespace = \is_scalar($config['namespace'] ?? null) ? (string) $config['namespace'] : null;

            $services->set('messenger_workflow.result_storage.redis')
                ->lazy()
                ->class(RedisResultStorage::class)
                ->arg('$redis', service($clientService))
                ->arg('$expireInputAfter', \is_int($config['expire_input_after'] ?? null) ? $config['expire_input_after'] : 3 * 60 * 60)
                ->arg('$expireAfterAwait', \is_int($config['expire_after_await'] ?? null) ? $config['expire_after_await'] : null)
                ->arg('$namespace', $namespace);
        }

        $services->alias('messenger_workflow.result_storage', 'messenger_workflow.result_storage.'.$provider);
        $services->alias(ResultStorageInterface::class, 'messenger_workflow.result_storage');
    }

    /**
     * Autoconfigures classes/methods carrying the given handler attribute as messenger
     * message handlers bound to the given bus (unless the attribute sets its own bus).
     *
     * @param class-string $attributeClass
     */
    private function registerAttributeMessengerHandler(ContainerBuilder $builder, string $attributeClass, string $bus): void
    {
        $builder->registerAttributeForAutoconfiguration($attributeClass, static function (ChildDefinition $definition, object $attribute, \Reflector $reflector) use ($attributeClass, $bus): void {
            $tagAttributes = get_object_vars($attribute);
            $tagAttributes['bus'] ??= $bus;
            $tagAttributes['from_transport'] = $tagAttributes['fromTransport'] ?? null;
            unset($tagAttributes['fromTransport']);

            if ($reflector instanceof \ReflectionMethod) {
                if (isset($tagAttributes['method'])) {
                    throw new \LogicException(\sprintf('%s attribute cannot declare a method on "%s::%s()".', $attributeClass, $reflector->class, $reflector->name));
                }

                $tagAttributes['method'] = $reflector->getName();
            }

            $definition->addTag('messenger.message_handler', $tagAttributes);
        });
    }
}
