<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Functional;

use Kraz\MessengerWorkflow\Application\Messenger\CommandBusInterface;
use Kraz\MessengerWorkflow\Application\Messenger\QueryBusInterface;
use Kraz\MessengerWorkflow\Application\Task\TaskOwnershipRegistryInterface;
use Kraz\MessengerWorkflow\Application\Task\TaskResultProviderInterface;
use Kraz\MessengerWorkflow\Application\Task\TaskStatusProviderInterface;
use Kraz\MessengerWorkflow\Infrastructure\Task\RedisTaskOwnershipRegistry;
use Kraz\MessengerWorkflow\Infrastructure\Task\RedisTaskResultProvider;
use Kraz\MessengerWorkflow\Infrastructure\Task\RedisTaskStatusProvider;
use Kraz\MessengerWorkflow\Infrastructure\Task\TrackingCommandBus;
use Kraz\MessengerWorkflow\Infrastructure\Task\TrackingQueryBus;
use Kraz\MessengerWorkflow\Tests\Support\WorkflowKernelTestCase;
use Kraz\MessengerWorkflow\Tests\TestKernel\TasksKernel;

/**
 * Task-services container wiring: tasks.enabled registers the Redis task services and
 * transparently decorates the package buses with the tracking decorators.
 */
final class TaskServicesWiringTest extends WorkflowKernelTestCase
{
    protected static function getKernelClass(): string
    {
        return TasksKernel::class;
    }

    public function testTaskServicePortsResolveToTheRedisAdapters(): void
    {
        $container = self::getContainer();

        self::assertInstanceOf(RedisTaskOwnershipRegistry::class, $container->get(TaskOwnershipRegistryInterface::class));
        self::assertInstanceOf(RedisTaskStatusProvider::class, $container->get(TaskStatusProviderInterface::class));
        self::assertInstanceOf(RedisTaskResultProvider::class, $container->get(TaskResultProviderInterface::class));
    }

    public function testTheBusesAreDecoratedWithTheTrackingDecorators(): void
    {
        $container = self::getContainer();

        self::assertInstanceOf(TrackingCommandBus::class, $container->get(CommandBusInterface::class));
        self::assertInstanceOf(TrackingQueryBus::class, $container->get(QueryBusInterface::class));
    }
}
