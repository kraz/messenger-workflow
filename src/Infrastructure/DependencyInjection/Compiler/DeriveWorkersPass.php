<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Infrastructure\DependencyInjection\Compiler;

use Kraz\MessengerWorkflow\Infrastructure\Worker\WorkerSetDeriver;
use Symfony\Component\DependencyInjection\Compiler\CompilerPassInterface;
use Symfony\Component\DependencyInjection\ContainerBuilder;

/**
 * Computes the final worker set: derives the required workers from the
 * configured transport topology, merges the manual workflow.workers entries and
 * publishes the result as the "messenger_workflow.workers" parameter consumed by
 * the messenger:supervisor-config command.
 *
 * Runs as a compiler pass because application modules declare their transports from
 * their own prependExtension(), which executes after this bundle's extension.
 */
final class DeriveWorkersPass implements CompilerPassInterface
{
    public function process(ContainerBuilder $container): void
    {
        $transportDsns = [];
        foreach ($container->getExtensionConfig('framework') as $frameworkConfig) {
            $messengerConfig = \is_array($frameworkConfig['messenger'] ?? null) ? $frameworkConfig['messenger'] : [];
            $transports = \is_array($messengerConfig['transports'] ?? null) ? $messengerConfig['transports'] : [];
            foreach ($transports as $name => $transport) {
                $dsn = \is_string($transport) ? $transport : (\is_array($transport) && \is_string($transport['dsn'] ?? null) ? $transport['dsn'] : null);
                if (\is_string($dsn)) {
                    $transportDsns[(string) $name] = $dsn;
                }
            }
        }

        $queueBindings = [];
        $workflowTransports = $container->hasParameter('messenger_workflow.messenger.transports')
            ? $container->getParameter('messenger_workflow.messenger.transports')
            : [];
        foreach (\is_array($workflowTransports) ? $workflowTransports : [] as $broker => $transportConfig) {
            $bindings = \is_array($transportConfig) && \is_array($transportConfig['queue_bindings'] ?? null) ? $transportConfig['queue_bindings'] : [];
            $queueBindings[(string) $broker] = $bindings;
        }

        $manualWorkers = $container->hasParameter('messenger_workflow.workflow.workers')
            ? $container->getParameter('messenger_workflow.workflow.workers')
            : [];
        /** @var list<array<array-key, mixed>> $manualWorkers */
        $manualWorkers = \is_array($manualWorkers) ? array_values(array_filter($manualWorkers, \is_array(...))) : [];

        $container->setParameter(
            'messenger_workflow.workers',
            new WorkerSetDeriver()->derive($transportDsns, $queueBindings, $manualWorkers),
        );
    }
}
