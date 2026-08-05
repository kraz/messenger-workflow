<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\TestKernel;

use Kraz\MessengerWorkflow\Tests\Fixture\RedisClientFactory;
use Symfony\Component\DependencyInjection\Loader\Configurator\ContainerConfigurator;

/**
 * TestKernel variant using the redis result storage provider backed by the local
 * development Redis (database 15).
 */
class RedisResultStorageKernel extends TestKernel
{
    protected function configureContainer(ContainerConfigurator $container): void
    {
        parent::configureContainer($container);

        $container->extension('messenger_workflow', [
            'messenger' => [
                'result_storage' => [
                    'provider' => 'redis',
                    'service' => 'test_redis_client',
                    'namespace' => 'fk',
                ],
            ],
        ]);

        $container->services()
            ->set('test_redis_client', \Redis::class)
            ->factory([RedisClientFactory::class, 'create']);
    }
}
