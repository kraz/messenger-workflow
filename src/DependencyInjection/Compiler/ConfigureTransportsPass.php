<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\DependencyInjection\Compiler;

use Kraz\MessengerWorkflow\Messenger\Amqp\AmqpTransportFactory;
use Kraz\MessengerWorkflow\Messenger\RoutingKey;
use Symfony\Component\DependencyInjection\Argument\IteratorArgument;
use Symfony\Component\DependencyInjection\Compiler\CompilerPassInterface;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\DependencyInjection\Definition;
use Symfony\Component\DependencyInjection\Reference;

final class ConfigureTransportsPass implements CompilerPassInterface
{
    public function process(ContainerBuilder $container): void
    {
        $this->configureMessenger($container);
    }

    private function configureMessenger(ContainerBuilder $container): void
    {
        $baseAmqpTransportFactoryId = \Jwage\PhpAmqpLibMessengerBundle\Transport\AmqpTransportFactory::class;
        if ($container->hasDefinition($baseAmqpTransportFactoryId)) {
            $amqpConnectionFactory = $container->getDefinition($baseAmqpTransportFactoryId)
                ->getArgument(0);

            $amqpTransportFactoryDefinition = new Definition(AmqpTransportFactory::class);
            $amqpTransportFactoryDefinition->setArgument('$connectionFactory', $amqpConnectionFactory);
            $amqpTransportFactoryDefinition->setArgument('$resultStorage', new Reference('messenger_workflow.result_storage'));
            $amqpTransportFactoryDefinition->addTag('messenger.transport_factory');

            $container->setDefinition($amqpTransportFactoryDefinition->getClass(), $amqpTransportFactoryDefinition);
            $container->removeDefinition($baseAmqpTransportFactoryId);
        }

        $this->configureMessageBrokerForTransport('events', $container, true);
        $this->configureMessageBrokerForTransport('commands', $container);
        $this->configureMessageBrokerForTransport('queries', $container);

        foreach (['event.bus', 'inbox.bus', 'outbox.bus'] as $busName) {
            if ($container->hasDefinition($busName)) {
                $this->removeMessageBusMiddleware($container->getDefinition($busName), [
                    'messenger.middleware.reject_redelivered_message_middleware',
                ]);
            }
        }
    }

    private function configureMessageBrokerForTransport(string $name, ContainerBuilder $container, bool $multicast = false): void
    {
        $transportServiceId = 'messenger.transport.'.$name;
        if (!$container->hasDefinition($transportServiceId)) {
            return;
        }

        $definition = $container->getDefinition($transportServiceId);
        $definitionArgs = $definition->getArguments();
        $exchangeType = $definitionArgs[1]['exchange']['type'] ?? null;
        $transport = $container->getParameter('messenger_workflow.messenger.transports')[$name] ?? [];
        $options = $definitionArgs[1] ?? [];
        foreach ($transport['queue_bindings'] ?? [] as $queueName => $queueBinding) {
            $owner = $queueBinding['owner'] ?? null;
            if (!$owner) {
                throw new \RuntimeException(\sprintf('Can not configure messenger transport "%s". The owner of queue "%s" is not set!', $name, $queueName));
            }
            $bindingKeys = $queueBinding['binding_keys'] ?? [];
            array_unshift($bindingKeys, $owner);
            if (!$multicast) {
                array_unshift($bindingKeys, $name.'.'.$owner);
            }
            $bindingKeys = array_unique(array_map(function (string $value) use ($owner, $name, $exchangeType) {
                $self = $owner === $value;

                return (string) match ($exchangeType) {
                    'topic' => RoutingKey::createForTopicTransport(($self ? 'internal.' : '').$value, $name, $self ? '#' : ''),
                    'direct' => RoutingKey::createForDirectTransport(($self ? 'internal.' : '').$value, $name),
                };
            }, $bindingKeys));
            $options['queues'][$queueName]['binding_keys'] = $bindingKeys;
        }
        $definitionArgs[1] = $options;
        $definition->setArguments($definitionArgs);
    }

    /**
     * @param string[] $middleware
     */
    private function removeMessageBusMiddleware(Definition $busDefinition, array $middleware): void
    {
        /** @var IteratorArgument $busMiddlewareReferences */
        $busMiddlewareReferences = $busDefinition->getArgument(0);
        $busMiddlewareStack = array_filter($busMiddlewareReferences->getValues(), function ($item) use ($middleware) {
            if ($item instanceof Reference) {
                return !\in_array((string) $item, $middleware, true);
            }

            return true;
        });
        $busDefinition->replaceArgument(0, new IteratorArgument(array_values($busMiddlewareStack)));
    }
}
