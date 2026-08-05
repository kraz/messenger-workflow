<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\TestKernel;

use Doctrine\Bundle\DoctrineBundle\DoctrineBundle;
use Jwage\PhpAmqpLibMessengerBundle\PhpAmqpLibMessengerBundle;
use Kraz\MessengerWorkflow\MessengerWorkflowBundle;
use Symfony\Bundle\FrameworkBundle\FrameworkBundle;
use Symfony\Bundle\FrameworkBundle\Kernel\MicroKernelTrait;
use Symfony\Component\DependencyInjection\Loader\Configurator\ContainerConfigurator;
use Symfony\Component\HttpKernel\Kernel;

/**
 * Config-BC kernel: loads the VERBATIM module and application YAML of the
 * BookAppDemo1 reference app (copied unchanged to tests/Fixture/BookAppDemo1).
 * The kernel itself only supplies what the reference app defines elsewhere: broker
 * transport DSNs, the Doctrine connections and the snc_redis client service.
 */
class BookAppDemo1BcKernel extends Kernel
{
    use MicroKernelTrait;

    /**
     * @return iterable<\Symfony\Component\HttpKernel\Bundle\BundleInterface>
     */
    public function registerBundles(): iterable
    {
        yield new FrameworkBundle();
        yield new DoctrineBundle();
        yield new PhpAmqpLibMessengerBundle();
        yield new MessengerWorkflowBundle();
    }

    public function getCacheDir(): string
    {
        return MWF_TEST_VAR_DIR.'/cache/'.static::class.'/'.$this->environment;
    }

    public function getLogDir(): string
    {
        return MWF_TEST_VAR_DIR.'/log';
    }

    protected function configureContainer(ContainerConfigurator $container): void
    {
        $fixtureDir = \dirname(__DIR__).'/Fixture/BookAppDemo1';
        $container->import($fixtureDir.'/BookStore/messenger.yaml');
        $container->import($fixtureDir.'/BookWarehouse/messenger.yaml');
        $container->import($fixtureDir.'/messenger_workflow.yaml');

        $container->extension('framework', [
            'secret' => 'test',
            'test' => true,
            'http_method_override' => false,
            'php_errors' => ['log' => true],
            'messenger' => [
                'transports' => [
                    'commands' => ['dsn' => '%env(MWF_TEST_AMQP_DSN)%'],
                    'queries' => ['dsn' => '%env(MWF_TEST_AMQP_DSN)%'],
                    'events' => ['dsn' => '%env(MWF_TEST_AMQP_DSN)%'],
                ],
            ],
        ]);

        $container->extension('doctrine', [
            'dbal' => [
                'default_connection' => 'book_store',
                'connections' => [
                    'book_store' => [
                        'driver' => 'pdo_sqlite',
                        'path' => MWF_TEST_VAR_DIR.'/db-book-store.sqlite',
                        'server_version' => '3.45.0',
                    ],
                    'book_warehouse' => [
                        'driver' => 'pdo_sqlite',
                        'path' => MWF_TEST_VAR_DIR.'/db-book-warehouse.sqlite',
                        'server_version' => '3.45.0',
                    ],
                ],
            ],
        ]);

        // The reference app registers this client through the snc/redis-bundle.
        $container->services()
            ->set('snc_redis.mwf_cache', \Redis::class);
    }
}
