<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\TestKernel;

use Kraz\MessengerWorkflow\Tests\Fixture\ResultStorageConsumer;
use Symfony\Component\DependencyInjection\Loader\Configurator\ContainerConfigurator;

class ResultStorageAutowireKernel extends TestKernel
{
    protected function configureExtraServices(ContainerConfigurator $container): void
    {
        $container->services()
            ->set(ResultStorageConsumer::class)
            ->autowire()
            ->public();
    }
}
