<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\TestKernel;

use Doctrine\Bundle\DoctrineBundle\DoctrineBundle;
use Jwage\PhpAmqpLibMessengerBundle\PhpAmqpLibMessengerBundle;
use Kraz\MessengerWorkflow\MessengerWorkflowBundle;
use Kraz\MessengerWorkflow\Tests\Fixture\Handler\MiniCommandHandler;
use Kraz\MessengerWorkflow\Tests\Fixture\Handler\MiniEventHandler;
use Kraz\MessengerWorkflow\Tests\Fixture\Handler\NanoCommandHandler;
use Kraz\MessengerWorkflow\Tests\Fixture\RedisClientFactory;
use Symfony\Bundle\FrameworkBundle\FrameworkBundle;
use Symfony\Bundle\FrameworkBundle\Kernel\MicroKernelTrait;
use Symfony\Component\DependencyInjection\Loader\Configurator\ContainerConfigurator;
use Symfony\Component\HttpKernel\Kernel;

use function Symfony\Component\DependencyInjection\Loader\Configurator\service;

/**
 * Reduced-flow topology (flow-flexibility matrix): NO inbox transports at all.
 *
 * Contexts:
 * - "Mini" (queue mini_commands / mini_events): outboxes + a command notifier, broker
 *   queues consumed directly; mini_commands is orm-mapped so the workflow transaction
 *   middleware opens a plain transaction around its handlers ("no inbox" row).
 * - "Nano" (queue nano_commands): nothing but the broker — no outbox, no notifier, no
 *   orm mapping ("minimal" row; tracked results are written directly by the handler
 *   worker).
 */
class ReducedFlowKernel extends Kernel
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
                    'commands' => ['dsn' => '%env(MWF_TEST_AMQP_DSN)%'],
                    'queries' => ['dsn' => '%env(MWF_TEST_AMQP_DSN)%'],
                    'events' => ['dsn' => '%env(MWF_TEST_AMQP_DSN)%'],
                    'mini_outbox' => ['dsn' => 'commands-outbox://postgres?table_name=zz_mini_outbox&get_notify_timeout=250&check_delayed_interval=250'],
                    'mini_events_outbox' => ['dsn' => 'events-outbox://postgres?table_name=zz_mini_events_outbox&get_notify_timeout=250&check_delayed_interval=250'],
                    // Notifier for the mini_commands queue — consumed directly from the
                    // broker, the "<queue>_notifier" convention still applies.
                    'mini_commands_notifier' => ['dsn' => 'commands-outbox://postgres?table_name=zz_mini_notifier&get_notify_timeout=250&check_delayed_interval=250'],
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
                            ['queue' => 'mini_commands', 'owner' => 'Mini'],
                            ['queue' => 'nano_commands', 'owner' => 'Nano'],
                        ],
                        // "postgres" is a DBAL connection name (accepted fallback when
                        // no entity manager of that name exists).
                        'orm_mappings' => [
                            'mini_commands' => ['orm' => 'postgres'],
                        ],
                    ],
                    'events' => [
                        'queue_bindings' => [
                            ['queue' => 'mini_events', 'owner' => 'Mini', 'binding_keys' => ['Contracts\Mini\Event\MiniEvent']],
                        ],
                    ],
                ],
                'outbox_buses' => [
                    'mini' => 'mini_events_outbox',
                ],
                'result_storage' => [
                    'provider' => 'redis',
                    'service' => 'test_redis_client',
                    'namespace' => 'fk',
                ],
            ],
        ]);

        $services = $container->services()->defaults()->autowire()->autoconfigure();
        $services->set('test_redis_client', \Redis::class)
            ->factory([RedisClientFactory::class, 'create']);
        $services->set(MiniCommandHandler::class)
            ->arg('$postgres', service('doctrine.dbal.postgres_connection'));
        $services->set(NanoCommandHandler::class)
            ->arg('$postgres', service('doctrine.dbal.postgres_connection'));
        $services->set(MiniEventHandler::class)
            ->arg('$postgres', service('doctrine.dbal.postgres_connection'));
    }
}
