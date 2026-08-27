<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Infrastructure\DependencyInjection\Compiler;

use Symfony\Component\DependencyInjection\Compiler\CompilerPassInterface;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\DependencyInjection\Reference;

/**
 * Compiles the `DBAL connection name → entity manager names` map consumed by the
 * transaction middleware. Each `doctrine.orm.<em>_entity_manager` definition references
 * its `doctrine.dbal.<connection>_connection` service, so the set of managers taking
 * part in a message transaction is fully known at compile time: the middleware flushes
 * exactly the managers of the transaction's connection — one hash lookup per message,
 * instead of instantiating and scanning every registered manager, a cost that grows
 * with the number of bounded contexts.
 *
 * Also logs a compiler warning for every transactional inbox transport whose connection
 * has no entity manager while the boundary flush is enabled. That is legitimate for a
 * DBAL-only context — and suspicious everywhere else, because a handler writing through
 * an ORM entity manager on another connection writes OUTSIDE the message transaction.
 */
final class ResolveTransactionEntityManagersPass implements CompilerPassInterface
{
    private const string MIDDLEWARE_ID = 'messenger_workflow.transaction_middleware';

    /**
     * Inbox DSN prefix → default of the transactional_handler option (mirrors the
     * corresponding transport factory).
     */
    private const array INBOX_DSN_DEFAULTS = [
        'inbox://' => false,
        'commands-inbox://' => true,
        'events-inbox://' => false,
    ];

    public function process(ContainerBuilder $container): void
    {
        if (!$container->hasDefinition(self::MIDDLEWARE_ID)) {
            return;
        }

        $map = $this->buildConnectionEntityManagersMap($container);
        $middleware = $container->getDefinition(self::MIDDLEWARE_ID);
        $middleware->setArgument('$connectionEntityManagers', $map);

        $flushEnabled = false !== ($middleware->getArguments()['$flushEntityManagers'] ?? true);
        if ($flushEnabled && [] !== $map) {
            $this->warnAboutTransactionalInboxesWithoutManagers($container, $map);
        }
    }

    /**
     * @return array<string, list<string>>
     */
    private function buildConnectionEntityManagersMap(ContainerBuilder $container): array
    {
        $entityManagers = $container->hasParameter('doctrine.entity_managers') ? $container->getParameter('doctrine.entity_managers') : [];
        $connections = $container->hasParameter('doctrine.connections') ? $container->getParameter('doctrine.connections') : [];

        $connectionNamesByServiceId = [];
        foreach (\is_array($connections) ? $connections : [] as $connectionName => $serviceId) {
            if (\is_string($serviceId)) {
                $connectionNamesByServiceId[$serviceId] = (string) $connectionName;
            }
        }

        $map = [];
        foreach (\is_array($entityManagers) ? $entityManagers : [] as $managerName => $serviceId) {
            if (!\is_string($serviceId) || !$container->hasDefinition($serviceId)) {
                continue;
            }
            foreach ($container->getDefinition($serviceId)->getArguments() as $argument) {
                if ($argument instanceof Reference && isset($connectionNamesByServiceId[(string) $argument])) {
                    $map[$connectionNamesByServiceId[(string) $argument]][] = (string) $managerName;
                    break;
                }
            }
        }

        return $map;
    }

    /**
     * @param array<string, list<string>> $map
     */
    private function warnAboutTransactionalInboxesWithoutManagers(ContainerBuilder $container, array $map): void
    {
        $defaultConnection = $container->hasParameter('doctrine.default_connection') ? $container->getParameter('doctrine.default_connection') : null;

        foreach ($container->getExtensionConfig('framework') as $frameworkConfig) {
            $messengerConfig = \is_array($frameworkConfig['messenger'] ?? null) ? $frameworkConfig['messenger'] : [];
            $transports = \is_array($messengerConfig['transports'] ?? null) ? $messengerConfig['transports'] : [];
            foreach ($transports as $name => $transport) {
                $dsn = \is_string($transport) ? $transport : (\is_array($transport) && \is_string($transport['dsn'] ?? null) ? $transport['dsn'] : null);
                if (null === $dsn || str_contains($dsn, '%')) {
                    continue; // a parameter/env-var DSN is not resolvable at compile time
                }

                $scheme = explode('://', $dsn, 2)[0].'://';
                if (!\array_key_exists($scheme, self::INBOX_DSN_DEFAULTS)) {
                    continue;
                }

                $dsnQuery = [];
                $rawQuery = parse_url($dsn, \PHP_URL_QUERY);
                parse_str(\is_string($rawQuery) ? $rawQuery : '', $dsnQuery);
                $options = \is_array($transport) && \is_array($transport['options'] ?? null) ? $transport['options'] : [];
                $transactional = filter_var(
                    $options['transactional_handler'] ?? $dsnQuery['transactional_handler'] ?? self::INBOX_DSN_DEFAULTS[$scheme],
                    \FILTER_VALIDATE_BOOL,
                );
                if (!$transactional) {
                    continue;
                }

                $host = parse_url($dsn, \PHP_URL_HOST);
                $connectionName = \is_string($host) && '' !== $host ? $host : (\is_string($defaultConnection) ? $defaultConnection : null);
                if (null === $connectionName || [] !== ($map[$connectionName] ?? [])) {
                    continue;
                }

                $container->log($this, \sprintf('The transactional inbox transport "%s" runs on the DBAL connection "%s", which has no ORM entity manager: the message-boundary flush has nothing to write there. That is fine for a DBAL-only context — but if this context uses the ORM, its entity manager is configured on a DIFFERENT connection and its changes would commit outside the message transaction.', (string) $name, $connectionName));
            }
        }
    }
}
