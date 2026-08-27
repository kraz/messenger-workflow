<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Unit\Infrastructure\DependencyInjection;

use Kraz\MessengerWorkflow\Infrastructure\DependencyInjection\Compiler\ResolveTransactionEntityManagersPass;
use PHPUnit\Framework\TestCase;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\DependencyInjection\Definition;
use Symfony\Component\DependencyInjection\Reference;

/**
 * Spec: the `connection name → entity manager names` map consumed by the transaction
 * middleware is compiled from DoctrineBundle's container state — each entity manager
 * definition references its connection service — so the middleware never scans manager
 * instances at runtime. Transactional inbox transports on a connection without any
 * entity manager are flagged in the compiler log while the boundary flush is enabled.
 */
final class ResolveTransactionEntityManagersPassTest extends TestCase
{
    /**
     * @param array<string, string>                        $entityManagerConnections em name → connection name
     * @param array<array-key, mixed>                      $messengerTransports      framework messenger transports config
     * @param array<string, mixed>                         $middlewareArguments
     */
    private function buildContainer(array $entityManagerConnections, array $messengerTransports = [], array $middlewareArguments = []): ContainerBuilder
    {
        $container = new ContainerBuilder();

        $connections = [];
        $entityManagers = [];
        foreach ($entityManagerConnections as $managerName => $connectionName) {
            $connectionServiceId = \sprintf('doctrine.dbal.%s_connection', $connectionName);
            if (!$container->hasDefinition($connectionServiceId)) {
                $container->setDefinition($connectionServiceId, new Definition(\stdClass::class));
            }
            $connections[$connectionName] = $connectionServiceId;

            $managerServiceId = \sprintf('doctrine.orm.%s_entity_manager', $managerName);
            $managerDefinition = new Definition(\stdClass::class);
            // Mirrors DoctrineBundle: connection reference first, configuration after.
            $managerDefinition->setArguments([
                new Reference($connectionServiceId),
                new Reference(\sprintf('doctrine.orm.%s_configuration', $managerName)),
            ]);
            $container->setDefinition($managerServiceId, $managerDefinition);
            $entityManagers[$managerName] = $managerServiceId;
        }

        $container->setParameter('doctrine.connections', $connections);
        $container->setParameter('doctrine.entity_managers', $entityManagers);
        $container->setParameter('doctrine.default_connection', array_key_first($connections) ?? 'default');

        $middleware = new Definition(\stdClass::class);
        foreach ($middlewareArguments as $key => $value) {
            $middleware->setArgument($key, $value);
        }
        $container->setDefinition('messenger_workflow.transaction_middleware', $middleware);

        if ([] !== $messengerTransports) {
            $container->prependExtensionConfig('framework', ['messenger' => ['transports' => $messengerTransports]]);
        }

        return $container;
    }

    private function compiledMap(ContainerBuilder $container): mixed
    {
        return $container->getDefinition('messenger_workflow.transaction_middleware')
            ->getArguments()['$connectionEntityManagers'] ?? null;
    }

    /**
     * @return list<string>
     */
    private function compilerLog(ContainerBuilder $container): array
    {
        return array_values(array_filter($container->getCompiler()->getLog(), \is_string(...)));
    }

    public function testTheMapGroupsEntityManagersByConnectionName(): void
    {
        $container = $this->buildContainer([
            'sales' => 'sales',
            'sales_reporting' => 'sales',
            'billing' => 'billing',
        ]);

        (new ResolveTransactionEntityManagersPass())->process($container);

        self::assertSame([
            'sales' => ['sales', 'sales_reporting'],
            'billing' => ['billing'],
        ], $this->compiledMap($container));
    }

    public function testATransactionalInboxOnAManagerlessConnectionIsFlaggedInTheCompilerLog(): void
    {
        $container = $this->buildContainer(
            ['sales' => 'sales'],
            ['billing_commands' => 'commands-inbox://billing'],
        );

        (new ResolveTransactionEntityManagersPass())->process($container);

        $log = implode("\n", $this->compilerLog($container));
        self::assertStringContainsString('billing_commands', $log);
        self::assertStringContainsString('connection "billing"', $log);
    }

    public function testAnInboxOnAConnectionWithManagersIsNotFlagged(): void
    {
        $container = $this->buildContainer(
            ['sales' => 'sales'],
            ['sales_commands' => 'commands-inbox://sales'],
        );

        (new ResolveTransactionEntityManagersPass())->process($container);

        self::assertSame([], $this->compilerLog($container));
    }

    public function testANonTransactionalInboxIsNotFlagged(): void
    {
        $container = $this->buildContainer(
            ['sales' => 'sales'],
            // events-inbox defaults to transactional_handler=false
            ['billing_events' => 'events-inbox://billing'],
        );

        (new ResolveTransactionEntityManagersPass())->process($container);

        self::assertSame([], $this->compilerLog($container));
    }

    public function testAnExplicitlyTransactionalEventsInboxIsFlagged(): void
    {
        $container = $this->buildContainer(
            ['sales' => 'sales'],
            ['billing_events' => 'events-inbox://billing?transactional_handler=true'],
        );

        (new ResolveTransactionEntityManagersPass())->process($container);

        self::assertStringContainsString('billing_events', implode("\n", $this->compilerLog($container)));
    }

    public function testADisabledBoundaryFlushSuppressesTheWarning(): void
    {
        $container = $this->buildContainer(
            ['sales' => 'sales'],
            ['billing_commands' => 'commands-inbox://billing'],
            ['$flushEntityManagers' => false],
        );

        (new ResolveTransactionEntityManagersPass())->process($container);

        self::assertSame([], $this->compilerLog($container));
    }

    public function testWithoutDoctrineParametersTheMapIsEmpty(): void
    {
        $container = new ContainerBuilder();
        $container->setDefinition('messenger_workflow.transaction_middleware', new Definition(\stdClass::class));

        (new ResolveTransactionEntityManagersPass())->process($container);

        self::assertSame([], $this->compiledMap($container));
    }
}
