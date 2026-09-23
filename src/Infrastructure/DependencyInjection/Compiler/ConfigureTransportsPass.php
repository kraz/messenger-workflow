<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Infrastructure\DependencyInjection\Compiler;

use Kraz\MessengerWorkflow\Application\Attribute\MessageRoute;
use Kraz\MessengerWorkflow\Application\CommandInterface;
use Kraz\MessengerWorkflow\Application\QueryInterface;
use Kraz\MessengerWorkflow\Infrastructure\Messenger\RoutingKey;
use Symfony\Component\DependencyInjection\Compiler\CompilerPassInterface;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\DependencyInjection\Reference;

/**
 * Computes the RabbitMQ queue binding keys for the broker transports from the
 * `messenger_workflow.messenger.transports.<name>.queue_bindings` configuration and
 * writes them into the transport options (`options.queues.<queue>.binding_keys`).
 *
 * Binding keys per queue: the owner context (own internal messages; on topic exchanges
 * as a `.#` wildcard), plus — direct exchanges only — the owner's public key, plus any
 * explicitly configured keys, plus — topic exchanges only — the message classes handled
 * by handlers bound to the queue via `from_transport`.
 *
 * Routed queues (direct exchanges only): a binding declaring `route: <name>` binds to
 * the owner keys suffixed with the route (`commands.internal.Warehouse.planning`) and to
 * nothing else, so a message carrying the route reaches this queue exclusively. The
 * pass compiles the `message class → route` map (from the bindings' `messages` lists
 * and the #[MessageRoute] attributes of locally handled classes) for the AMQP stamp
 * factory, resolves the notifier outbox of every routed commands queue for the
 * notifier middleware, gives a routed inbox declared without a table its own default
 * table, and validates the whole arrangement — including that no two inbox or outbox
 * transports share a storage table on one connection.
 */
final class ConfigureTransportsPass implements CompilerPassInterface
{
    private const array WORKFLOW_DSN_PREFIXES = [
        'outbox://', 'commands-outbox://', 'events-outbox://',
        'inbox://', 'commands-inbox://', 'events-inbox://',
    ];

    private const array INBOX_DSN_PREFIXES = ['inbox://', 'commands-inbox://', 'events-inbox://'];
    private const array OUTBOX_DSN_PREFIXES = ['outbox://', 'commands-outbox://', 'events-outbox://'];

    /**
     * Mirrors the transport factories' per-scheme table defaults.
     */
    private const array DEFAULT_TABLE_NAMES = [
        'inbox://' => 'z_inbox',
        'commands-inbox://' => 'zz_commands_inbox',
        'events-inbox://' => 'zz_events_inbox',
        'outbox://' => 'z_outbox',
        'commands-outbox://' => 'zz_commands_outbox',
        'events-outbox://' => 'zz_events_outbox',
    ];

    /**
     * The direct-exchange brokers that support routes, with the marker interface a
     * routed message class must implement.
     */
    private const array ROUTABLE_BROKERS = [
        'commands' => CommandInterface::class,
        'queries' => QueryInterface::class,
    ];

    private const string ROUTE_RESOLVER_ID = 'messenger_workflow.message_route_resolver';
    private const string NOTIFIER_MIDDLEWARE_ID = 'messenger_workflow.command_notifier_middleware';

    public function process(ContainerBuilder $container): void
    {
        $this->assertNoOrderingConflicts($container);
        $this->configureOutboxRetries($container);
        $this->configureCommandRetryStrategies($container);

        $transports = $this->collectFrameworkTransports($container);
        $routedInboxTables = $this->configureMessageRoutes($container, $transports);
        $this->applyDerivedInboxTables($container, $transports, $routedInboxTables);
        $this->assertNoSharedStorageTables($transports);

        $this->configureMessageBrokerForTransport('events', $container, true);
        $this->configureMessageBrokerForTransport('commands', $container);
        $this->configureMessageBrokerForTransport('queries', $container);
    }

    /**
     * Boot-time guard: a workflow transport configured with both
     * strict_order and multiple_consumers is a contradiction — ordered delivery
     * requires a single FIFO consumer. Failing at container compile time beats the
     * runtime failure in the (lazy) transport factory, which only the consuming
     * worker would hit.
     */
    private function assertNoOrderingConflicts(ContainerBuilder $container): void
    {
        foreach ($container->getExtensionConfig('framework') as $frameworkConfig) {
            $messengerConfig = \is_array($frameworkConfig['messenger'] ?? null) ? $frameworkConfig['messenger'] : [];
            $transports = \is_array($messengerConfig['transports'] ?? null) ? $messengerConfig['transports'] : [];
            foreach ($transports as $name => $transport) {
                $dsn = \is_string($transport) ? $transport : (\is_array($transport) && \is_string($transport['dsn'] ?? null) ? $transport['dsn'] : null);
                if (null === $dsn || !\in_array(explode('://', $dsn, 2)[0].'://', self::WORKFLOW_DSN_PREFIXES, true)) {
                    continue;
                }

                $dsnQuery = [];
                $rawQuery = parse_url($dsn, \PHP_URL_QUERY);
                parse_str(\is_string($rawQuery) ? $rawQuery : '', $dsnQuery);
                $options = \is_array($transport) && \is_array($transport['options'] ?? null) ? $transport['options'] : [];

                $strictOrder = filter_var($dsnQuery['strict_order'] ?? $options['strict_order'] ?? false, \FILTER_VALIDATE_BOOL);
                $multipleConsumers = filter_var($dsnQuery['multiple_consumers'] ?? $options['multiple_consumers'] ?? false, \FILTER_VALIDATE_BOOL);
                if ($strictOrder && $multipleConsumers) {
                    throw new \LogicException(\sprintf('Invalid configuration of messenger transport "%s": the "strict_order" and "multiple_consumers" options are mutually exclusive — ordered delivery requires a single FIFO consumer.', \is_string($name) ? $name : (string) $name));
                }
            }
        }
    }

    /**
     * Outbox transports must never use Symfony's retry mechanism: a retry re-SENDS the
     * envelope to the transport (inserting a NEW outbox row) and rejects the original —
     * which the outbox receiver deliberately keeps — duplicating the message. With
     * max_retries=0 a relay failure goes straight to reject(), which keeps the row
     * (retry_count incremented) for the next relay attempt.
     *
     * Runs as a compiler pass because application modules declare their outbox transports
     * from their own prependExtension(), which executes after this bundle's prepend. The
     * default stays overridable via an explicit "retry_strategy.max_retries".
     */
    private function configureOutboxRetries(ContainerBuilder $container): void
    {
        $outboxTransports = [];
        $explicitMaxRetries = [];
        foreach ($container->getExtensionConfig('framework') as $frameworkConfig) {
            $messengerConfig = \is_array($frameworkConfig['messenger'] ?? null) ? $frameworkConfig['messenger'] : [];
            $transports = \is_array($messengerConfig['transports'] ?? null) ? $messengerConfig['transports'] : [];
            foreach ($transports as $name => $transport) {
                $dsn = \is_string($transport) ? $transport : (\is_array($transport) && \is_string($transport['dsn'] ?? null) ? $transport['dsn'] : null);
                if (\is_string($dsn) && (str_starts_with($dsn, 'outbox://') || str_starts_with($dsn, 'commands-outbox://') || str_starts_with($dsn, 'events-outbox://'))) {
                    $outboxTransports[$name] = true;
                }
                $retryStrategy = \is_array($transport) && \is_array($transport['retry_strategy'] ?? null) ? $transport['retry_strategy'] : [];
                if (\array_key_exists('max_retries', $retryStrategy)) {
                    $explicitMaxRetries[$name] = true;
                }
            }
        }

        foreach (array_keys($outboxTransports) as $name) {
            if (isset($explicitMaxRetries[$name])) {
                continue;
            }
            $retryServiceId = 'messenger.retry.multiplier_retry_strategy.'.$name;
            if ($container->hasDefinition($retryServiceId)) {
                $container->getDefinition($retryServiceId)->replaceArgument(0, 0);
            }
        }
    }

    /**
     * Applies the workflow retry policies as the default retry strategy of the
     * matching inbox transports: commands-inbox → command policy (no retries except
     * transient errors), events-inbox → event policy (always retried within a bounded
     * total budget). An explicit "retry_strategy" on the transport config keeps full
     * control with the application.
     */
    private function configureCommandRetryStrategies(ContainerBuilder $container): void
    {
        if (!$container->hasDefinition('messenger.retry_strategy_locator')) {
            return;
        }

        $strategyByScheme = [
            'commands-inbox://' => 'messenger_workflow.command_retry_strategy',
            'events-inbox://' => 'messenger_workflow.event_retry_strategy',
        ];

        // Queries have no inbox segment — their policy applies to the broker transport
        // itself (retry delays run through the AMQP delay exchange there).
        $strategyByTransport = $container->hasDefinition('messenger_workflow.query_retry_strategy')
            ? ['queries' => 'messenger_workflow.query_retry_strategy']
            : [];
        $explicitRetryStrategy = [];
        $inboxSchemesPresent = [];
        foreach ($container->getExtensionConfig('framework') as $frameworkConfig) {
            $messengerConfig = \is_array($frameworkConfig['messenger'] ?? null) ? $frameworkConfig['messenger'] : [];
            $transports = \is_array($messengerConfig['transports'] ?? null) ? $messengerConfig['transports'] : [];
            foreach ($transports as $name => $transport) {
                $dsn = \is_string($transport) ? $transport : (\is_array($transport) && \is_string($transport['dsn'] ?? null) ? $transport['dsn'] : null);
                foreach ($strategyByScheme as $scheme => $strategyId) {
                    if (\is_string($dsn) && str_starts_with($dsn, $scheme) && $container->hasDefinition($strategyId)) {
                        $strategyByTransport[$name] = $strategyId;
                        $inboxSchemesPresent[$scheme] = true;
                    }
                }
                if (\is_array($transport) && \array_key_exists('retry_strategy', $transport)) {
                    $explicitRetryStrategy[$name] = true;
                }
            }
        }

        // No-inbox mode: without any inbox transport of a flow, consuming
        // the broker transport IS handler execution, so the flow's retry policy applies
        // to the broker transport itself (delays run through the AMQP delay exchange).
        // With inboxes present, the broker transport only feeds the receiver relay and
        // keeps its stock/explicit retry strategy.
        $brokerByScheme = ['commands-inbox://' => 'commands', 'events-inbox://' => 'events'];
        foreach ($strategyByScheme as $scheme => $strategyId) {
            $broker = $brokerByScheme[$scheme];
            if (!isset($inboxSchemesPresent[$scheme]) && !isset($strategyByTransport[$broker]) && $container->hasDefinition($strategyId)) {
                $strategyByTransport[$broker] = $strategyId;
            }
        }

        $locator = $container->getDefinition('messenger.retry_strategy_locator');
        $references = $locator->getArgument(0);
        if (!\is_array($references)) {
            return;
        }

        foreach ($strategyByTransport as $name => $strategyId) {
            if (isset($explicitRetryStrategy[$name]) || !\array_key_exists($name, $references)) {
                continue;
            }
            $references[$name] = new Reference($strategyId);
        }

        $locator->replaceArgument(0, $references);
    }

    /**
     * The framework.messenger.transports declarations as seen across every prepended
     * or loaded framework config: DSN (last declaration wins) and merged options.
     *
     * @return array<string, array{dsn: string|null, options: array<array-key, mixed>}>
     */
    private function collectFrameworkTransports(ContainerBuilder $container): array
    {
        $collected = [];
        foreach ($container->getExtensionConfig('framework') as $frameworkConfig) {
            $messengerConfig = \is_array($frameworkConfig['messenger'] ?? null) ? $frameworkConfig['messenger'] : [];
            $transports = \is_array($messengerConfig['transports'] ?? null) ? $messengerConfig['transports'] : [];
            foreach ($transports as $name => $transport) {
                $name = (string) $name;
                $dsn = \is_string($transport) ? $transport : (\is_array($transport) && \is_string($transport['dsn'] ?? null) ? $transport['dsn'] : null);
                $options = \is_array($transport) && \is_array($transport['options'] ?? null) ? $transport['options'] : [];
                $collected[$name] = [
                    'dsn' => $dsn ?? ($collected[$name]['dsn'] ?? null),
                    'options' => array_replace($collected[$name]['options'] ?? [], $options),
                ];
            }
        }

        return $collected;
    }

    /**
     * Compiles the routes of the direct-exchange brokers: validates every routed queue
     * binding, builds the `message class → route` map for the stamp factory and the
     * `queue → notifier` map for the notifier middleware.
     *
     * @param array<string, array{dsn: string|null, options: array<array-key, mixed>}> $transports
     *
     * @return array<string, string> routed inbox transport name → derived default table name
     */
    private function configureMessageRoutes(ContainerBuilder $container, array $transports): array
    {
        $transportsParameter = $container->hasParameter('messenger_workflow.messenger.transports')
            ? $container->getParameter('messenger_workflow.messenger.transports')
            : [];
        $transportsParameter = \is_array($transportsParameter) ? $transportsParameter : [];

        $classRoutes = [];
        $classQueues = [];
        $notifierByQueue = [];
        $routedInboxTables = [];
        $handledClasses = null;

        foreach (self::ROUTABLE_BROKERS as $broker => $messageInterface) {
            $brokerConfig = \is_array($transportsParameter[$broker] ?? null) ? $transportsParameter[$broker] : [];
            $queueBindings = \is_array($brokerConfig['queue_bindings'] ?? null) ? $brokerConfig['queue_bindings'] : [];
            if ([] === $queueBindings) {
                continue;
            }

            $describe = static fn (string $queue): string => \sprintf('Invalid configuration of messenger transport "%s", queue "%s"', $broker, $queue);

            // Index the regular (unrouted) queue of every owner and the routed queues.
            $regularByOwner = [];
            $routed = [];
            foreach ($queueBindings as $queue => $binding) {
                $queue = (string) $queue;
                $binding = \is_array($binding) ? $binding : [];
                $owner = \is_string($binding['owner'] ?? null) && '' !== $binding['owner'] ? $binding['owner'] : null;
                if (null === $owner) {
                    throw new \RuntimeException(\sprintf('Can not configure messenger transport "%s". The owner of queue "%s" is not set!', $broker, $queue));
                }

                $route = $binding['route'] ?? null;
                if (null === $route) {
                    if (isset($regularByOwner[$owner])) {
                        throw new \LogicException(\sprintf('%s: the owner "%s" is already the owner of queue "%s". On a direct exchange two queues of one owner bind to the same keys and every message is delivered to both — declare a "route" on the queue that must receive only selected message classes.', $describe($queue), $owner, $regularByOwner[$owner]));
                    }
                    $regularByOwner[$owner] = $queue;
                    continue;
                }

                if (!\is_string($route) || 1 !== preg_match(RoutingKey::ROUTE_PATTERN, $route)) {
                    throw new \LogicException(\sprintf('%s: the route must be one routing-key segment (letters, digits and underscores), got %s.', $describe($queue), \is_scalar($route) ? '"'.$route.'"' : get_debug_type($route)));
                }
                if (isset($routed[$owner][$route])) {
                    throw new \LogicException(\sprintf('%s: the route "%s" of owner "%s" is already served by queue "%s" — two queues bound to one route key would both receive every routed message.', $describe($queue), $route, $owner, $routed[$owner][$route]));
                }
                if ([] !== ($binding['binding_keys'] ?? [])) {
                    throw new \LogicException(\sprintf('%s: a routed queue binds to its route keys only; "binding_keys" cannot be combined with "route".', $describe($queue)));
                }
                $routed[$owner][$route] = $queue;
            }

            $routeMembers = [];
            foreach ($routed as $owner => $routes) {
                $ownerKeys = $this->ownerKeys($owner, $broker);
                foreach ($routes as $route => $queue) {
                    $binding = \is_array($queueBindings[$queue] ?? null) ? $queueBindings[$queue] : [];
                    $regularQueue = $regularByOwner[$owner] ?? null;
                    if (null === $regularQueue) {
                        throw new \LogicException(\sprintf('%s: the route "%s" splits selected message classes off the regular queue of owner "%s", but no queue without a route is declared for that owner on this transport — every unrouted message of the context would be unroutable and dropped by the broker.', $describe($queue), $route, $owner));
                    }

                    $queueHasInbox = $this->hasInboxTransport($transports, $queue);
                    $regularHasInbox = $this->hasInboxTransport($transports, $regularQueue);
                    if ($queueHasInbox !== $regularHasInbox) {
                        throw new \LogicException(\sprintf('%s: the routed queue %s an inbox transport named after it while the regular queue "%s" of owner "%s" %s one. A context handles all its queues in the same mode: the flow retry policy sits either on the inboxes or on the broker transport, and a queue without an inbox silently loses deduplication and the handler transaction.', $describe($queue), $queueHasInbox ? 'has' : 'has no', $regularQueue, $owner, $regularHasInbox ? 'has' : 'has no'));
                    }

                    $messages = $binding['messages'] ?? [];
                    if (!\is_array($messages)) {
                        throw new \LogicException(\sprintf('%s: "messages" must be a list of message class names.', $describe($queue)));
                    }
                    foreach ($messages as $class) {
                        if (!\is_string($class) || '' === $class) {
                            throw new \LogicException(\sprintf('%s: "messages" must be a list of message class names, got %s.', $describe($queue), get_debug_type($class)));
                        }
                        $class = ltrim($class, '\\');
                        $this->assertRoutableMessageClass($class, $messageInterface, $ownerKeys, $owner, $broker, $describe($queue));
                        if (isset($classRoutes[$class]) && $classQueues[$class] !== $queue) {
                            throw new \LogicException(\sprintf('%s: the message class "%s" is already routed to queue "%s" (route "%s") — a message class can be assigned to one route only.', $describe($queue), $class, $classQueues[$class], $classRoutes[$class]));
                        }
                        $attributeRoute = $this->attributeRouteOf($class);
                        if (null !== $attributeRoute && $attributeRoute !== $route) {
                            throw new \LogicException(\sprintf('%s: the message class "%s" is listed for route "%s" but declares #[MessageRoute(\'%s\')] — the configuration and the attribute disagree.', $describe($queue), $class, $route, $attributeRoute));
                        }
                        $classRoutes[$class] = $route;
                        $classQueues[$class] = $queue;
                        $routeMembers[$owner][$route] = ($routeMembers[$owner][$route] ?? 0) + 1;
                    }

                    if ('commands' === $broker) {
                        $notifierByQueue[$queue] = $this->resolveRoutedNotifier($queue, $binding, $regularQueue, $transports, $describe($queue));
                    }

                    $derivedTable = $queueHasInbox ? $this->derivedInboxTable($transports, $queue) : null;
                    if (null !== $derivedTable) {
                        $routedInboxTables[$queue] = $derivedTable;
                    }
                }
            }

            // #[MessageRoute] on classes handled in this container: validated against
            // the declared routes and added to the compiled map.
            $handledClasses ??= $this->findHandledMessageClasses($container);
            foreach ($handledClasses as $class) {
                if (isset($classRoutes[$class]) || !is_a($class, $messageInterface, true)) {
                    continue;
                }
                $route = $this->attributeRouteOf($class);
                if (null === $route) {
                    continue;
                }
                $classKey = (string) RoutingKey::createForDirectTransport($class, $broker);
                $ownerOfClass = null;
                foreach ($routed as $owner => $routes) {
                    if (isset($routes[$route]) && \in_array($classKey, $this->ownerKeys($owner, $broker), true)) {
                        $ownerOfClass = $owner;
                        break;
                    }
                }
                if (null === $ownerOfClass) {
                    throw new \LogicException(\sprintf('Invalid configuration of messenger transport "%s": the handled message class "%s" declares #[MessageRoute(\'%s\')], but no queue with route "%s" is declared for its bounded context (routing key "%s") — its messages would be unroutable and dropped by the broker.', $broker, $class, $route, $route, $classKey));
                }
                $classRoutes[$class] = $route;
                $classQueues[$class] = $routed[$ownerOfClass][$route];
                $routeMembers[$ownerOfClass][$route] = ($routeMembers[$ownerOfClass][$route] ?? 0) + 1;
            }

            foreach ($routed as $owner => $routes) {
                foreach ($routes as $route => $queue) {
                    if (0 === ($routeMembers[$owner][$route] ?? 0)) {
                        throw new \LogicException(\sprintf('%s: the route "%s" routes no message class — list the classes under "messages" or mark them with #[MessageRoute(\'%s\')].', $describe($queue), $route, $route));
                    }
                }
            }
        }

        if ($container->hasDefinition(self::ROUTE_RESOLVER_ID)) {
            $container->getDefinition(self::ROUTE_RESOLVER_ID)->setArgument('$routes', $classRoutes);
        }
        if ($container->hasDefinition(self::NOTIFIER_MIDDLEWARE_ID)) {
            $container->getDefinition(self::NOTIFIER_MIDDLEWARE_ID)->setArgument('$notifierByQueue', $notifierByQueue);
        }

        return $routedInboxTables;
    }

    /**
     * @return list<string> the owner's public and internal routing keys on the broker
     */
    private function ownerKeys(string $owner, string $broker): array
    {
        return [
            (string) RoutingKey::createForDirectTransport($owner, $broker),
            (string) RoutingKey::createForDirectTransport('internal.'.$owner, $broker),
        ];
    }

    /**
     * @param class-string $messageInterface
     * @param list<string> $ownerKeys
     */
    private function assertRoutableMessageClass(string $class, string $messageInterface, array $ownerKeys, string $owner, string $broker, string $context): void
    {
        if (!class_exists($class) && !interface_exists($class)) {
            throw new \LogicException(\sprintf('%s: the routed message class "%s" does not exist.', $context, $class));
        }
        if (!is_a($class, $messageInterface, true)) {
            throw new \LogicException(\sprintf('%s: the routed message class "%s" must implement "%s".', $context, $class, $messageInterface));
        }
        $classKey = (string) RoutingKey::createForDirectTransport($class, $broker);
        if (!\in_array($classKey, $ownerKeys, true)) {
            throw new \LogicException(\sprintf('%s: the routed message class "%s" belongs to another bounded context than the queue owner "%s" (its routing key is "%s", the owner keys are "%s").', $context, $class, $owner, $classKey, implode('", "', $ownerKeys)));
        }
    }

    /**
     * The route declared by a #[MessageRoute] attribute on the class, its parents or
     * its interfaces (mirrors MessageRouteResolver's attribute lookup).
     */
    private function attributeRouteOf(string $class): ?string
    {
        if (!class_exists($class) && !interface_exists($class)) {
            return null;
        }
        $parents = class_parents($class);
        $interfaces = class_implements($class);
        $candidates = [$class, ...array_values(false === $parents ? [] : $parents), ...array_values(false === $interfaces ? [] : $interfaces)];
        foreach ($candidates as $candidate) {
            $attributes = new \ReflectionClass($candidate)->getAttributes(MessageRoute::class);
            if ([] !== $attributes) {
                return $attributes[0]->newInstance()->route;
            }
        }

        return null;
    }

    /**
     * The notifier outbox of a routed commands queue: an explicit "notifier" on the
     * binding (false/null = the reduced flow on purpose), else the queue's own
     * "<queue>_notifier", else the "<regular queue>_notifier" of the context — the
     * default, and the recommended shape: one notifier outbox and one notifier worker
     * per context, the routed queue only publishes its results through it. Null when
     * the context runs without a notifier altogether.
     *
     * @param array<array-key, mixed>                                                   $binding
     * @param array<string, array{dsn: string|null, options: array<array-key, mixed>}> $transports
     */
    private function resolveRoutedNotifier(string $queue, array $binding, string $regularQueue, array $transports, string $context): ?string
    {
        if (\array_key_exists('notifier', $binding)) {
            $notifier = $binding['notifier'];
            if (null === $notifier || false === $notifier) {
                return null;
            }
            if (!\is_string($notifier) || '' === $notifier) {
                throw new \LogicException(\sprintf('%s: "notifier" must be the name of a commands outbox transport, or false for the reduced flow without a notifier.', $context));
            }
            if (!$this->hasOutboxTransport($transports, $notifier)) {
                throw new \LogicException(\sprintf('%s: the notifier "%s" is not a configured outbox transport.', $context, $notifier));
            }
            $this->assertNotifierSharesInboxConnection($transports, $queue, $notifier, $context);

            return $notifier;
        }

        foreach ([$queue.'_notifier', $regularQueue.'_notifier'] as $candidate) {
            if (!isset($transports[$candidate])) {
                continue;
            }
            if (!$this->hasOutboxTransport($transports, $candidate)) {
                throw new \LogicException(\sprintf('%s: the notifier transport "%s" must be an outbox transport.', $context, $candidate));
            }
            $this->assertNotifierSharesInboxConnection($transports, $queue, $candidate, $context);

            return $candidate;
        }

        return null;
    }

    /**
     * A transactional inbox publishes the completion notification inside the handler
     * transaction, which requires the notifier outbox on the inbox's DBAL connection.
     *
     * @param array<string, array{dsn: string|null, options: array<array-key, mixed>}> $transports
     */
    private function assertNotifierSharesInboxConnection(array $transports, string $inbox, string $notifier, string $context): void
    {
        $inboxDsn = $transports[$inbox]['dsn'] ?? null;
        $notifierDsn = $transports[$notifier]['dsn'] ?? null;
        if (null === $inboxDsn || null === $notifierDsn || str_contains($inboxDsn, '%') || str_contains($notifierDsn, '%') || !$this->hasInboxTransport($transports, $inbox)) {
            return;
        }

        $inboxHost = parse_url($inboxDsn, \PHP_URL_HOST);
        $notifierHost = parse_url($notifierDsn, \PHP_URL_HOST);
        if ($inboxHost !== $notifierHost) {
            throw new \LogicException(\sprintf('%s: the notifier outbox "%s" (connection "%s") must use the same DBAL connection as the inbox transport "%s" (connection "%s") so the completion notification is written inside the handler transaction.', $context, $notifier, \is_string($notifierHost) ? $notifierHost : '', $inbox, \is_string($inboxHost) ? $inboxHost : ''));
        }
    }

    /**
     * @param array<string, array{dsn: string|null, options: array<array-key, mixed>}> $transports
     */
    private function hasInboxTransport(array $transports, string $name): bool
    {
        $dsn = $transports[$name]['dsn'] ?? null;

        return null !== $dsn && \in_array(explode('://', $dsn, 2)[0].'://', self::INBOX_DSN_PREFIXES, true);
    }

    /**
     * @param array<string, array{dsn: string|null, options: array<array-key, mixed>}> $transports
     */
    private function hasOutboxTransport(array $transports, string $name): bool
    {
        $dsn = $transports[$name]['dsn'] ?? null;

        return null !== $dsn && \in_array(explode('://', $dsn, 2)[0].'://', self::OUTBOX_DSN_PREFIXES, true);
    }

    /**
     * The default table of a routed inbox declared without one: the scheme default
     * suffixed with the transport name ("zz_commands_inbox_warehouse_planning"). A routed
     * inbox is new by definition, so deriving its table renames nothing; the scheme
     * defaults of every other transport stay untouched.
     *
     * @param array<string, array{dsn: string|null, options: array<array-key, mixed>}> $transports
     */
    private function derivedInboxTable(array $transports, string $name): ?string
    {
        $dsn = $transports[$name]['dsn'] ?? null;
        if (null === $dsn || str_contains($dsn, '%')) {
            return null;
        }
        $scheme = explode('://', $dsn, 2)[0].'://';
        $dsnQuery = [];
        $rawQuery = parse_url($dsn, \PHP_URL_QUERY);
        parse_str(\is_string($rawQuery) ? $rawQuery : '', $dsnQuery);
        if (isset($dsnQuery['table_name']) || isset($transports[$name]['options']['table_name']) || !isset(self::DEFAULT_TABLE_NAMES[$scheme])) {
            return null;
        }

        return self::DEFAULT_TABLE_NAMES[$scheme].'_'.$name;
    }

    /**
     * Writes the derived default tables into the transport definitions (what the
     * transport factory and messenger:setup-transports see) and into the collected
     * declarations (what the shared-table guard compares).
     *
     * @param array<string, array{dsn: string|null, options: array<array-key, mixed>}> $transports
     * @param array<string, string>                                                      $routedInboxTables
     *
     * @param-out array<string, array{dsn: string|null, options: array<array-key, mixed>}> $transports
     */
    private function applyDerivedInboxTables(ContainerBuilder $container, array &$transports, array $routedInboxTables): void
    {
        foreach ($routedInboxTables as $name => $table) {
            if (isset($transports[$name])) {
                $transports[$name]['options']['table_name'] = $table;
            }

            $serviceId = 'messenger.transport.'.$name;
            if (!$container->hasDefinition($serviceId)) {
                continue;
            }
            $definition = $container->getDefinition($serviceId);
            $arguments = $definition->getArguments();
            $options = \is_array($arguments[1] ?? null) ? $arguments[1] : [];
            $options['table_name'] ??= $table;
            $arguments[1] = $options;
            $definition->setArguments($arguments);
        }
    }

    /**
     * Boot-time guard: the workflow Doctrine transports default their table per DSN
     * scheme, so two inbox (or outbox) transports on one connection declared without
     * an explicit table_name silently share one table — one pile of rows consumed by
     * both workers, one dedup index, one NOTIFY channel. Nothing fails at runtime; the
     * transports just stop being distinct. The build fails instead.
     *
     * @param array<string, array{dsn: string|null, options: array<array-key, mixed>}> $transports
     */
    private function assertNoSharedStorageTables(array $transports): void
    {
        $byTable = [];
        $byIndexTable = [];
        foreach ($transports as $name => ['dsn' => $dsn, 'options' => $options]) {
            if (null === $dsn || str_contains($dsn, '%')) {
                continue; // a parameter/env-var DSN is not resolvable at compile time
            }
            $scheme = explode('://', $dsn, 2)[0].'://';
            if (!isset(self::DEFAULT_TABLE_NAMES[$scheme])) {
                continue;
            }

            $dsnQuery = [];
            $rawQuery = parse_url($dsn, \PHP_URL_QUERY);
            parse_str(\is_string($rawQuery) ? $rawQuery : '', $dsnQuery);
            $host = parse_url($dsn, \PHP_URL_HOST);
            $connection = \is_string($host) ? $host : '';

            // Same precedence as Connection::buildConfiguration(): DSN query, options, default.
            $table = $dsnQuery['table_name'] ?? $options['table_name'] ?? self::DEFAULT_TABLE_NAMES[$scheme];
            $table = \is_scalar($table) ? (string) $table : self::DEFAULT_TABLE_NAMES[$scheme];
            $indexTable = $dsnQuery['index_table_name'] ?? $options['index_table_name'] ?? $table.'_index';
            $indexTable = \is_scalar($indexTable) ? (string) $indexTable : $table.'_index';

            $byTable[$connection][$table][] = $name;
            $byIndexTable[$connection][$indexTable][] = $name;
        }

        foreach ($byTable as $connection => $tables) {
            foreach ($tables as $table => $names) {
                if (\count($names) > 1) {
                    throw new \LogicException(\sprintf('Invalid configuration of messenger transports "%s": they share the storage table "%s" on the DBAL connection "%s". Every inbox and outbox transport needs a table of its own — the workflow transports default the table per DSN scheme, not per transport. Give all but one of them an explicit table (for example "?table_name=%s").', implode('", "', $names), $table, $connection, $table.'_'.$names[1]));
                }
            }
        }
        foreach ($byIndexTable as $connection => $tables) {
            foreach ($tables as $indexTable => $names) {
                if (\count($names) > 1) {
                    throw new \LogicException(\sprintf('Invalid configuration of messenger transports "%s": they share the deduplication index table "%s" on the DBAL connection "%s" — a message id recorded by one transport would be dropped by the other. Give each transport its own "index_table_name" (or let it default to "<table_name>_index").', implode('", "', $names), $indexTable, $connection));
                }
            }
        }
    }

    private function configureMessageBrokerForTransport(string $name, ContainerBuilder $container, bool $multicast = false): void
    {
        $transportServiceId = 'messenger.transport.'.$name;
        if (!$container->hasDefinition($transportServiceId)) {
            return;
        }

        $transportsParameter = $container->hasParameter('messenger_workflow.messenger.transports')
            ? $container->getParameter('messenger_workflow.messenger.transports')
            : [];
        $transport = \is_array($transportsParameter) && \is_array($transportsParameter[$name] ?? null) ? $transportsParameter[$name] : [];
        $queueBindings = \is_array($transport['queue_bindings'] ?? null) ? $transport['queue_bindings'] : [];
        if ([] === $queueBindings) {
            return;
        }

        $definition = $container->getDefinition($transportServiceId);
        $definitionArgs = $definition->getArguments();
        $options = \is_array($definitionArgs[1] ?? null) ? $definitionArgs[1] : [];
        $exchangeType = \is_array($options['exchange'] ?? null) && \is_string($options['exchange']['type'] ?? null)
            ? $options['exchange']['type']
            : null;

        foreach ($queueBindings as $queueName => $queueBinding) {
            $queueName = (string) $queueName;
            $queueBinding = \is_array($queueBinding) ? $queueBinding : [];
            $owner = \is_string($queueBinding['owner'] ?? null) && '' !== $queueBinding['owner'] ? $queueBinding['owner'] : null;
            if (null === $owner) {
                throw new \RuntimeException(\sprintf('Can not configure messenger transport "%s". The owner of queue "%s" is not set!', $name, $queueName));
            }
            $route = \is_string($queueBinding['route'] ?? null) && '' !== $queueBinding['route'] ? $queueBinding['route'] : null;
            if (null !== $route && $multicast) {
                throw new \LogicException(\sprintf('Invalid configuration of messenger transport "%s", queue "%s": routes are only supported on direct exchanges (commands, queries) — on the topic exchange, bind the queue to the event classes instead.', $name, $queueName));
            }

            $bindingKeys = \is_array($queueBinding['binding_keys'] ?? null) ? array_values(array_filter($queueBinding['binding_keys'], \is_string(...))) : [];
            if ($multicast) {
                $bindingKeys = array_merge($bindingKeys, $this->findHandledMessageClasses($container, $queueName));
            }
            array_unshift($bindingKeys, $owner);
            if (!$multicast) {
                array_unshift($bindingKeys, $name.'.'.$owner);
            }

            $bindingKeys = array_unique(array_map(function (string $value) use ($owner, $name, $exchangeType, $queueName, $route): string {
                $self = $owner === $value;

                $key = match ($exchangeType) {
                    'topic' => RoutingKey::createForTopicTransport(($self ? 'internal.' : '').$value, $name, $self ? '#' : ''),
                    'direct' => RoutingKey::createForDirectTransport(($self ? 'internal.' : '').$value, $name),
                    default => throw new \RuntimeException(\sprintf('Can not configure messenger transport "%s". The exchange type must be "direct" or "topic" to compute the binding keys of queue "%s", but "%s" is configured.', $name, $queueName, $exchangeType ?? 'null')),
                };

                return (string) (null === $route ? $key : $key->withRoute($route));
            }, $bindingKeys));

            $queues = \is_array($options['queues'] ?? null) ? $options['queues'] : [];
            $queueOptions = \is_array($queues[$queueName] ?? null) ? $queues[$queueName] : [];
            $queueOptions['binding_keys'] = $this->removeBindingKeysCoveredByWildcard($bindingKeys);
            $queues[$queueName] = $queueOptions;
            $options['queues'] = $queues;
        }

        $definitionArgs[1] = $options;
        $definition->setArguments($definitionArgs);
    }

    /**
     * Collects the message classes handled by every messenger handler — bound to the
     * given transport via from_transport, or all of them when no transport is given —
     * so their binding keys (or routes) can be registered without explicit configuration.
     *
     * @return list<string>
     */
    private function findHandledMessageClasses(ContainerBuilder $container, ?string $transportName = null): array
    {
        $classes = [];
        foreach ($container->findTaggedServiceIds('messenger.message_handler', true) as $serviceId => $tags) {
            foreach ($tags as $tag) {
                if (!\is_array($tag)) {
                    continue;
                }
                if (null !== $transportName && ($tag['from_transport'] ?? null) !== $transportName) {
                    continue;
                }
                if (\is_string($tag['handles'] ?? null) && '' !== $tag['handles']) {
                    $classes[] = $tag['handles'];
                    continue;
                }

                $handlerClass = $container->getParameterBag()->resolveValue($container->getDefinition($serviceId)->getClass());
                $reflection = \is_string($handlerClass) && '' !== $handlerClass ? $container->getReflectionClass($handlerClass, false) : null;
                $method = \is_string($tag['method'] ?? null) && '' !== $tag['method'] ? $tag['method'] : '__invoke';
                if (null === $reflection || !$reflection->hasMethod($method)) {
                    continue;
                }

                $type = ($reflection->getMethod($method)->getParameters()[0] ?? null)?->getType();
                $types = $type instanceof \ReflectionUnionType ? $type->getTypes() : [$type];
                foreach ($types as $parameterType) {
                    if ($parameterType instanceof \ReflectionNamedType && !$parameterType->isBuiltin()) {
                        $classes[] = $parameterType->getName();
                    }
                }
            }
        }

        return array_values(array_unique($classes));
    }

    /**
     * @param string[] $bindingKeys
     *
     * @return list<string>
     */
    private function removeBindingKeysCoveredByWildcard(array $bindingKeys): array
    {
        return array_values(array_filter($bindingKeys, static function (string $key) use ($bindingKeys): bool {
            foreach ($bindingKeys as $other) {
                if ($other !== $key && str_ends_with($other, '.#') && str_starts_with($key, substr($other, 0, -1))) {
                    return false;
                }
            }

            return true;
        }));
    }
}
