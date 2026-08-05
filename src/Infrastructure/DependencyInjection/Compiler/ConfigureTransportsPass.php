<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Infrastructure\DependencyInjection\Compiler;

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
 */
final class ConfigureTransportsPass implements CompilerPassInterface
{
    private const array WORKFLOW_DSN_PREFIXES = [
        'outbox://', 'commands-outbox://', 'events-outbox://',
        'inbox://', 'commands-inbox://', 'events-inbox://',
    ];

    public function process(ContainerBuilder $container): void
    {
        $this->assertNoOrderingConflicts($container);
        $this->configureOutboxRetries($container);
        $this->configureCommandRetryStrategies($container);

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

            $bindingKeys = \is_array($queueBinding['binding_keys'] ?? null) ? array_values(array_filter($queueBinding['binding_keys'], \is_string(...))) : [];
            if ($multicast) {
                $bindingKeys = array_merge($bindingKeys, $this->findHandledMessageClasses($container, $queueName));
            }
            array_unshift($bindingKeys, $owner);
            if (!$multicast) {
                array_unshift($bindingKeys, $name.'.'.$owner);
            }

            $bindingKeys = array_unique(array_map(function (string $value) use ($owner, $name, $exchangeType, $queueName): string {
                $self = $owner === $value;

                return (string) match ($exchangeType) {
                    'topic' => RoutingKey::createForTopicTransport(($self ? 'internal.' : '').$value, $name, $self ? '#' : ''),
                    'direct' => RoutingKey::createForDirectTransport(($self ? 'internal.' : '').$value, $name),
                    default => throw new \RuntimeException(\sprintf('Can not configure messenger transport "%s". The exchange type must be "direct" or "topic" to compute the binding keys of queue "%s", but "%s" is configured.', $name, $queueName, $exchangeType ?? 'null')),
                };
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
     * Collects the message classes handled by every messenger handler bound to the given
     * transport, so their binding keys can be registered without explicit configuration.
     *
     * @return list<string>
     */
    private function findHandledMessageClasses(ContainerBuilder $container, string $transportName): array
    {
        $classes = [];
        foreach ($container->findTaggedServiceIds('messenger.message_handler', true) as $serviceId => $tags) {
            foreach ($tags as $tag) {
                if (!\is_array($tag)) {
                    continue;
                }
                if (($tag['from_transport'] ?? null) !== $transportName) {
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
