<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\TestKernel;

use Kraz\MessengerWorkflow\Tests\Fixture\RedisClientFactory;
use Symfony\Component\DependencyInjection\Loader\Configurator\ContainerConfigurator;

class RedisResultStorageKernel extends TestKernel
{
    protected function messengerWorkflowConfig(): array
    {
        $config = parent::messengerWorkflowConfig();
        $config['messenger']['result_storage'] = [
            'provider' => 'redis',
            'service' => 'test_redis_client',
        ];

        return $config;
    }

    protected function configureExtraServices(ContainerConfigurator $container): void
    {
        $container->services()
            ->set('test_redis_client', \Redis::class)
            ->factory([RedisClientFactory::class, 'create']);
    }
}
