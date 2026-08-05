<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\TestKernel;

use Doctrine\Bundle\DoctrineBundle\DoctrineBundle;
use Jwage\PhpAmqpLibMessengerBundle\PhpAmqpLibMessengerBundle;
use Kraz\MessengerWorkflow\MessengerWorkflowBundle;
use Kraz\MessengerWorkflow\Tests\Fixture\Handler\ChaosCommandHandler;
use Symfony\Bundle\FrameworkBundle\FrameworkBundle;
use Symfony\Bundle\FrameworkBundle\Kernel\MicroKernelTrait;
use Symfony\Component\DependencyInjection\Loader\Configurator\ContainerConfigurator;
use Symfony\Component\HttpKernel\Kernel;

use function Symfony\Component\DependencyInjection\Loader\Configurator\service;

/**
 * Chaos-test topology. The broker DSN comes from the MWF_CHAOS_AMQP_DSN
 * environment variable, resolved at runtime — tests point it at a closed port to
 * simulate a broker outage and back at the real broker to simulate recovery
 * (a fresh kernel boot per step).
 *
 * The chaos_commands inbox uses redeliver_timeout=1 so crash-recovery redelivery is
 * observable within test time.
 */
class ChaosKernel extends Kernel
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
        $container->extension('framework', [
            'secret' => 'test',
            'test' => true,
            'http_method_override' => false,
            'php_errors' => ['log' => true],
            'messenger' => [
                'transports' => [
                    'commands' => ['dsn' => '%env(MWF_CHAOS_AMQP_DSN)%'],
                    'queries' => ['dsn' => '%env(MWF_CHAOS_AMQP_DSN)%'],
                    'events' => ['dsn' => '%env(MWF_CHAOS_AMQP_DSN)%'],
                    'chaos_outbox' => ['dsn' => 'commands-outbox://postgres?table_name=zz_chaos_outbox&get_notify_timeout=250&check_delayed_interval=250'],
                    'chaos_commands' => ['dsn' => 'commands-inbox://postgres?table_name=zz_chaos_cmd_inbox&redeliver_timeout=1&get_notify_timeout=250&check_delayed_interval=250'],
                ],
            ],
        ]);

        $pgEnv = static fn (string $name, string $default): string => \is_string($_ENV[$name] ?? null) && '' !== $_ENV[$name] ? $_ENV[$name] : $default;
        $container->extension('doctrine', [
            'dbal' => [
                'default_connection' => 'postgres',
                'connections' => [
                    'postgres' => [
                        'driver' => 'pdo_pgsql',
                        'host' => $pgEnv('MWF_TEST_PG_HOST', '127.0.0.1'),
                        'port' => (int) $pgEnv('MWF_TEST_PG_PORT', '5432'),
                        'user' => $pgEnv('MWF_TEST_PG_USER', 'test'),
                        'password' => $pgEnv('MWF_TEST_PG_PASSWORD', 'test'),
                        'dbname' => $pgEnv('MWF_TEST_PG_DBNAME', 'mwf_test'),
                        'server_version' => '18.4.0',
                    ],
                ],
            ],
        ]);

        $container->extension('messenger_workflow', [
            'messenger' => [
                'transports' => [
                    'commands' => [
                        'queue_bindings' => [
                            ['queue' => 'chaos_commands', 'owner' => 'Chaos'],
                        ],
                    ],
                ],
            ],
        ]);

        $container->services()->defaults()->autowire()->autoconfigure()
            ->set(ChaosCommandHandler::class)
            ->arg('$postgres', service('doctrine.dbal.postgres_connection'));
    }
}
