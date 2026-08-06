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
    private const array WORKFLOW_DSN_PREFIXES = [
        'outbox://', 'commands-outbox://', 'events-outbox://',
        'inbox://', 'commands-inbox://', 'events-inbox://',
    ];

    // The typed outbox factories force multiple_consumers=false — these sources can
    // never run more than one process.
    private const array FORCED_SINGLE_CONSUMER_DSN_PREFIXES = ['commands-outbox://', 'events-outbox://'];

    public function process(ContainerBuilder $container): void
    {
        $transportDsns = [];
        $transportOptions = [];
        foreach ($container->getExtensionConfig('framework') as $frameworkConfig) {
            $messengerConfig = \is_array($frameworkConfig['messenger'] ?? null) ? $frameworkConfig['messenger'] : [];
            $transports = \is_array($messengerConfig['transports'] ?? null) ? $messengerConfig['transports'] : [];
            foreach ($transports as $name => $transport) {
                $dsn = \is_string($transport) ? $transport : (\is_array($transport) && \is_string($transport['dsn'] ?? null) ? $transport['dsn'] : null);
                if (\is_string($dsn)) {
                    $transportDsns[(string) $name] = $dsn;
                }
                if (\is_array($transport) && \is_array($transport['options'] ?? null)) {
                    $transportOptions[(string) $name] = array_replace($transportOptions[(string) $name] ?? [], $transport['options']);
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

        $workers = new WorkerSetDeriver()->derive($transportDsns, $queueBindings, $manualWorkers);
        $this->assertInstancesMatchConsumerMode($workers, $transportDsns, $transportOptions, $this->defaultInstances($container));

        $container->setParameter('messenger_workflow.workers', $workers);
    }

    /**
     * Boot-time guard: in single-consumer mode the inbox/outbox get() takes no row
     * locks and never marks rows as delivered, so N processes consuming the same
     * source transport would process the same rows N times. A worker with
     * instances > 1 therefore requires multiple_consumers=true on its source.
     *
     * The effective instance count mirrors the supervisor command's render-time
     * merge: an explicit worker value wins, otherwise worker_defaults applies.
     *
     * @param list<array<array-key, mixed>> $workers
     * @param array<string, string>         $transportDsns
     * @param array<string, array<array-key, mixed>> $transportOptions
     */
    private function assertInstancesMatchConsumerMode(array $workers, array $transportDsns, array $transportOptions, int $defaultInstances): void
    {
        foreach ($workers as $worker) {
            $instances = is_numeric($worker['instances'] ?? null) ? (int) $worker['instances'] : $defaultInstances;
            if ($instances <= 1) {
                continue;
            }

            $source = \is_scalar($worker['source'] ?? null) ? (string) $worker['source'] : '';
            $dsn = $transportDsns[$source] ?? null;
            $scheme = null !== $dsn ? explode('://', $dsn, 2)[0].'://' : '';
            if (null === $dsn || !\in_array($scheme, self::WORKFLOW_DSN_PREFIXES, true)) {
                continue;
            }

            $forcedSingleConsumer = \in_array($scheme, self::FORCED_SINGLE_CONSUMER_DSN_PREFIXES, true);
            if (!$forcedSingleConsumer && $this->resolvesToMultipleConsumers($scheme, $dsn, $transportOptions[$source] ?? [])) {
                continue;
            }

            $name = \is_scalar($worker['name'] ?? null) && '' !== (string) $worker['name'] ? (string) $worker['name'] : $source;
            $remedy = $forcedSingleConsumer
                ? 'Outbox transports are always single-consumer — set "instances: 1".'
                : 'Set "instances: 1" or configure "multiple_consumers=true" on the transport (mutually exclusive with "strict_order").';
            throw new \LogicException(\sprintf('Invalid configuration of worker "%s": "instances: %d" on the single-consumer transport "%s" — in this mode consumers take no row locks and every process would handle the same messages. %s', $name, $instances, $source, $remedy));
        }
    }

    /**
     * Mirrors the transport factories' consumer-mode resolution: an explicit
     * multiple_consumers (DSN query or options) wins; commands-inbox defaults to
     * competing consumers unless strict_order suppresses it (CommandsInboxTransportFactory);
     * every other scheme defaults to single-consumer (Connection default).
     *
     * @param array<array-key, mixed> $options
     */
    private function resolvesToMultipleConsumers(string $scheme, string $dsn, array $options): bool
    {
        $dsnQuery = [];
        $rawQuery = parse_url($dsn, \PHP_URL_QUERY);
        parse_str(\is_string($rawQuery) ? $rawQuery : '', $dsnQuery);

        $explicit = $dsnQuery['multiple_consumers'] ?? $options['multiple_consumers'] ?? null;
        if (null !== $explicit) {
            return filter_var($explicit, \FILTER_VALIDATE_BOOL);
        }

        if ('commands-inbox://' === $scheme) {
            return !filter_var($options['strict_order'] ?? $dsnQuery['strict_order'] ?? false, \FILTER_VALIDATE_BOOL);
        }

        return false;
    }

    private function defaultInstances(ContainerBuilder $container): int
    {
        $defaults = $container->hasParameter('messenger_workflow.workflow.worker_defaults')
            ? $container->getParameter('messenger_workflow.workflow.worker_defaults')
            : [];

        return \is_array($defaults) && is_numeric($defaults['instances'] ?? null) ? (int) $defaults['instances'] : 1;
    }
}
