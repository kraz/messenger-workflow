<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\TestKernel;

use Kraz\MessengerWorkflow\Application\Task\TaskOwnerResolverInterface;
use Kraz\MessengerWorkflow\Tests\Fixture\FixedTaskOwnerResolver;
use Symfony\Component\DependencyInjection\Loader\Configurator\ContainerConfigurator;

/**
 * RedisResultStorageKernel variant with the task services enabled and a fixture
 * owner resolver.
 */
final class TasksKernel extends RedisResultStorageKernel
{
    protected function configureContainer(ContainerConfigurator $container): void
    {
        parent::configureContainer($container);

        $container->extension('messenger_workflow', [
            'messenger' => [
                'tasks' => [
                    'enabled' => true,
                    'debug' => true,
                ],
            ],
        ]);

        $container->services()
            ->set(FixedTaskOwnerResolver::class)
            ->alias(TaskOwnerResolverInterface::class, FixedTaskOwnerResolver::class);
    }
}
