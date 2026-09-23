<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\TestKernel;

use Contracts\Demo\Command\PlanSomethingCommand;
use Kraz\MessengerWorkflow\Tests\Fixture\Handler\PlanSomethingCommandHandler;
use Symfony\Component\DependencyInjection\Loader\Configurator\ContainerConfigurator;

/**
 * The full TestKernel topology plus a ROUTED commands queue of the Demo context:
 * "app_planning" (route "planning") receives PlanSomethingCommand (listed under
 * "messages") and ReplanSomethingCommand (#[MessageRoute] on the class), on a
 * single-consumer FIFO inbox declared WITHOUT a table name — the bundle derives one.
 * No "app_planning_notifier": tracked results go through "app_commands_notifier".
 */
class RoutedCommandsKernel extends RedisResultStorageKernel
{
    protected function configureContainer(ContainerConfigurator $container): void
    {
        parent::configureContainer($container);

        $container->extension('framework', [
            'messenger' => [
                'transports' => [
                    'app_planning' => [
                        'dsn' => 'commands-inbox://postgres?strict_order=true&get_notify_timeout=250&check_delayed_interval=250',
                        'failure_transport' => 'app_planning_failures',
                    ],
                    // Shares zz_commands_failures with app_commands_failures (filtered by queue_name).
                    'app_planning_failures' => ['dsn' => 'commands-failures://postgres?queue_name=app_planning'],
                ],
            ],
        ]);

        $container->extension('messenger_workflow', [
            'messenger' => [
                'transports' => [
                    'commands' => [
                        'queue_bindings' => [
                            ['queue' => 'app_planning', 'owner' => 'Demo', 'route' => 'planning', 'messages' => [PlanSomethingCommand::class]],
                        ],
                    ],
                ],
            ],
        ]);

        $container->services()->defaults()->autowire()->autoconfigure()
            ->set(PlanSomethingCommandHandler::class);
    }
}
