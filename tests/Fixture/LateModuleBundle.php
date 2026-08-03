<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Fixture;

use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\DependencyInjection\Loader\Configurator\ContainerConfigurator;
use Symfony\Component\HttpKernel\Bundle\AbstractBundle;

/**
 * Mimics an application module bundle: it contributes its own command inbox transport from
 * prependExtension(). Because it is registered after MessengerWorkflowBundle, its prepend runs
 * later than the bundle's prepend — reproducing the real application where a prepend-time scan of
 * the framework config could not see this transport. The "no retries" default must still be
 * applied to it (handled at compile time by ConfigureTransportsPass).
 */
final class LateModuleBundle extends AbstractBundle
{
    protected string $extensionAlias = 'late_module';

    public function prependExtension(ContainerConfigurator $container, ContainerBuilder $builder): void
    {
        $builder->prependExtensionConfig('framework', [
            'messenger' => [
                'transports' => [
                    'late_module_commands' => [
                        'dsn' => 'commands-inbox://app?auto_setup=true',
                        'failure_transport' => 'app_commands_failures',
                    ],
                ],
            ],
        ]);
    }
}
