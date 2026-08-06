<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Unit\Infrastructure\DependencyInjection;

use Kraz\MessengerWorkflow\Infrastructure\DependencyInjection\Compiler\DeriveWorkersPass;
use PHPUnit\Framework\TestCase;
use Symfony\Component\DependencyInjection\ContainerBuilder;

/**
 * Spec: the derived worker set must be safe to run — in single-consumer mode the
 * inbox/outbox get() takes no row locks and never marks rows as delivered, so a
 * worker with instances > 1 on such a source would process every message once per
 * process. The pass rejects that combination at container compile time.
 */
final class DeriveWorkersPassTest extends TestCase
{
    /**
     * @param array<string, mixed>          $frameworkTransports
     * @param list<array<array-key, mixed>> $manualWorkers
     * @param array<array-key, mixed>       $workerDefaults
     */
    private function buildContainer(array $frameworkTransports, array $manualWorkers = [], array $workerDefaults = []): ContainerBuilder
    {
        $container = new ContainerBuilder();
        $container->prependExtensionConfig('framework', [
            'messenger' => ['transports' => $frameworkTransports],
        ]);
        $container->setParameter('messenger_workflow.messenger.transports', []);
        $container->setParameter('messenger_workflow.workflow.workers', $manualWorkers);
        $container->setParameter('messenger_workflow.workflow.worker_defaults', $workerDefaults);

        return $container;
    }

    public function testInstancesAboveOneOnASingleConsumerInboxFailsAtBoot(): void
    {
        $container = $this->buildContainer(
            ['app_events' => 'events-inbox://default'],
            [['name' => 'app_events handler', 'type' => 'event_handler', 'source' => 'app_events', 'instances' => 2]],
        );

        $this->expectException(\LogicException::class);
        $this->expectExceptionMessageMatches('/"app_events handler".*"instances: 2".*single-consumer transport "app_events"/');

        new DeriveWorkersPass()->process($container);
    }

    public function testInstancesAboveOneOnASingleConsumerOutboxFailsAtBoot(): void
    {
        $container = $this->buildContainer(
            ['app_outbox' => 'events-outbox://default'],
            [['name' => 'app_outbox publisher', 'type' => 'event_publisher', 'source' => 'app_outbox', 'instances' => 3]],
        );

        $this->expectException(\LogicException::class);
        $this->expectExceptionMessageMatches('/"app_outbox publisher".*"instances: 3".*"app_outbox"/');

        new DeriveWorkersPass()->process($container);
    }

    public function testInstancesAboveOneWithMultipleConsumersInTheDsnIsAccepted(): void
    {
        $container = $this->buildContainer(
            ['app_commands' => 'commands-inbox://default?multiple_consumers=true'],
            [['name' => 'app_commands handler', 'type' => 'command_handler', 'source' => 'app_commands', 'instances' => 4]],
        );

        new DeriveWorkersPass()->process($container);

        self::assertIsArray($container->getParameter('messenger_workflow.workers'));
    }

    public function testInstancesAboveOneWithMultipleConsumersInTheOptionsIsAccepted(): void
    {
        $container = $this->buildContainer(
            ['app_commands' => ['dsn' => 'commands-inbox://default', 'options' => ['multiple_consumers' => true]]],
            [['name' => 'app_commands handler', 'type' => 'command_handler', 'source' => 'app_commands', 'instances' => 4]],
        );

        new DeriveWorkersPass()->process($container);

        self::assertIsArray($container->getParameter('messenger_workflow.workers'));
    }

    public function testInstancesAboveOneOnABrokerSourceIsAccepted(): void
    {
        // Broker queues are competing-consumer by design — never flagged.
        $container = $this->buildContainer(
            ['queries' => 'phpamqplib://guest:guest@localhost'],
            [['name' => 'app_queries handler', 'type' => 'query_handler', 'source' => 'queries', 'queue' => 'app_queries', 'instances' => 8]],
        );

        new DeriveWorkersPass()->process($container);

        self::assertIsArray($container->getParameter('messenger_workflow.workers'));
    }

    public function testWorkerDefaultsInstancesApplyToDerivedWorkers(): void
    {
        // The derived handler worker carries no explicit "instances"; the render-time
        // merge would give it the worker_defaults value, so the guard must use it too.
        $container = $this->buildContainer(
            ['app_events' => 'events-inbox://default'],
            [['name' => 'app_events handler', 'type' => 'event_handler', 'source' => 'app_events']],
            ['instances' => 2],
        );

        $this->expectException(\LogicException::class);
        $this->expectExceptionMessageMatches('/"app_events handler".*"instances: 2"/');

        new DeriveWorkersPass()->process($container);
    }

    public function testAnExplicitSingleInstanceWinsOverWorkerDefaults(): void
    {
        $container = $this->buildContainer(
            ['app_events' => 'events-inbox://default'],
            [['name' => 'app_events handler', 'type' => 'event_handler', 'source' => 'app_events', 'instances' => 1]],
            ['instances' => 2],
        );

        new DeriveWorkersPass()->process($container);

        self::assertIsArray($container->getParameter('messenger_workflow.workers'));
    }

    public function testSingleInstanceWorkersOnSingleConsumerSourcesAreAccepted(): void
    {
        $container = $this->buildContainer(
            ['app_events' => 'events-inbox://default?strict_order=true'],
            [['name' => 'app_events handler', 'type' => 'event_handler', 'source' => 'app_events', 'instances' => 1]],
        );

        new DeriveWorkersPass()->process($container);

        self::assertIsArray($container->getParameter('messenger_workflow.workers'));
    }
}
