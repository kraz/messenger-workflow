<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\TestKernel;

use Symfony\Component\DependencyInjection\Loader\Configurator\ContainerConfigurator;

/**
 * TestKernel variant with a tiny event retry budget so the poison-message test
 * exhausts retries quickly (max_retries=2 → 3 handler attempts, then DLQ).
 */
final class EventFlowKernel extends TestKernel
{
    protected function configureContainer(ContainerConfigurator $container): void
    {
        parent::configureContainer($container);

        $container->extension('messenger_workflow', [
            'messenger' => [
                'defaults' => [
                    'event_retry' => [
                        'max_retries' => 2,
                        'delay' => 50,
                        'max_delay' => 100,
                        'jitter' => 0.0,
                    ],
                ],
            ],
        ]);
    }
}
