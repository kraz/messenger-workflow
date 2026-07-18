<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\TestKernel;

use Doctrine\Bundle\DoctrineBundle\DoctrineBundle;
use Jwage\PhpAmqpLibMessengerBundle\PhpAmqpLibMessengerBundle;
use Kraz\MessengerWorkflow\MessengerWorkflowBundle;
use Kraz\MessengerWorkflow\Tests\Fixture\MessageRecorder;
use Symfony\Bundle\FrameworkBundle\FrameworkBundle;
use Symfony\Bundle\FrameworkBundle\Kernel\MicroKernelTrait;
use Symfony\Component\DependencyInjection\Loader\Configurator\ContainerConfigurator;
use Symfony\Component\HttpKernel\Kernel;

class TestKernel extends Kernel
{
    use MicroKernelTrait;

    public function registerBundles(): iterable
    {
        yield new FrameworkBundle();
        yield new DoctrineBundle();
        yield new PhpAmqpLibMessengerBundle();
        yield new MessengerWorkflowBundle();
    }

    public function getProjectDir(): string
    {
        return \dirname(__DIR__, 2);
    }

    public function getCacheDir(): string
    {
        return MWF_TEST_VAR_DIR.'/'.str_replace('\\', '_', static::class).'/cache/'.$this->environment;
    }

    public function getLogDir(): string
    {
        return MWF_TEST_VAR_DIR.'/'.str_replace('\\', '_', static::class).'/log';
    }

    protected function configureContainer(ContainerConfigurator $container): void
    {
        $container->extension('framework', [
            'secret' => 'test-secret',
            'test' => true,
            'http_method_override' => false,
            'php_errors' => ['log' => true],
            'serializer' => ['enabled' => true],
            'messenger' => [
                'transports' => $this->messengerTransports(),
            ],
        ]);

        $container->extension('doctrine', [
            'dbal' => [
                'default_connection' => 'app',
                'connections' => [
                    'app' => [
                        'driver' => 'pdo_sqlite',
                        'path' => $this->getCacheDir().'/app.db',
                    ],
                    'pg' => [
                        'driver' => 'pdo_pgsql',
                        'host' => '%env(MWF_TEST_PG_HOST)%',
                        'port' => '%env(int:MWF_TEST_PG_PORT)%',
                        'user' => '%env(MWF_TEST_PG_USER)%',
                        'password' => '%env(MWF_TEST_PG_PASSWORD)%',
                        'dbname' => '%env(MWF_TEST_PG_DBNAME)%',
                    ],
                ],
            ],
        ]);

        $container->extension('messenger_workflow', $this->messengerWorkflowConfig());

        $services = $container->services()
            ->defaults()
                ->autowire()
                ->autoconfigure();

        $services->set(MessageRecorder::class)->public();
        $services->set(\Kraz\MessengerWorkflow\Tests\Fixture\BusAccessor::class)->public();
        $services->load('Kraz\\MessengerWorkflow\\Tests\\Fixture\\Handler\\', \dirname(__DIR__).'/Fixture/Handler/')
            ->public();

        $this->configureExtraServices($container);
    }

    protected function messengerTransports(): array
    {
        $amqp = ['dsn' => '%env(MWF_TEST_AMQP_DSN)%'];

        return [
            // Broker transports; exchange topology comes from the bundle's prepended messenger.yaml
            'commands' => $amqp,
            'queries' => $amqp,
            'events' => $amqp,

            // Module transports as in smartflow-demo-1 (bounded context "app" on sqlite)
            'app_commands' => [
                'dsn' => 'commands-inbox://app?auto_setup=true',
                'failure_transport' => 'app_commands_failures',
            ],
            'app_commands_failures' => 'commands-failures://app?queue_name=app_commands',
            'app_commands_notifier' => 'commands-outbox://app?auto_setup=true',
            'app_outbox' => 'events-outbox://app?auto_setup=true',
            'app_events' => [
                'dsn' => 'events-inbox://app?auto_setup=true',
                'failure_transport' => 'app_events_failures',
            ],
            'app_events_failures' => 'events-failures://app?queue_name=app_events',
        ];
    }

    protected function messengerWorkflowConfig(): array
    {
        return [
            'messenger' => [
                'transports' => [
                    'events' => [
                        'queue_bindings' => [
                            [
                                'queue' => 'app_events',
                                'owner' => 'TestApp',
                                'binding_keys' => [
                                    'Contracts\Demo\Event\SomethingHappened',
                                ],
                            ],
                        ],
                    ],
                    'commands' => [
                        'queue_bindings' => [
                            ['queue' => 'app_commands', 'owner' => 'TestApp'],
                        ],
                    ],
                    'queries' => [
                        'queue_bindings' => [
                            ['queue' => 'app_queries', 'owner' => 'TestApp'],
                        ],
                    ],
                ],
                'workflow' => [
                    'worker_defaults' => [
                        'supervisor' => [
                            'autostart' => 'true',
                            'autorestart' => 'true',
                        ],
                    ],
                    'workers' => [
                        ['name' => 'App events publisher', 'group' => 'App', 'type' => 'event_publisher', 'source' => 'app_outbox'],
                        ['name' => 'App events receiver', 'group' => 'App', 'type' => 'event_receiver', 'queue' => 'app_events'],
                        ['name' => 'App events handler', 'group' => 'App', 'type' => 'event_handler', 'source' => 'app_events'],
                        ['name' => 'App commands receiver', 'group' => 'App', 'type' => 'command_receiver', 'queue' => 'app_commands', 'instances' => 2],
                        ['name' => 'App commands handler', 'group' => 'App', 'type' => 'command_handler', 'source' => 'app_commands'],
                        ['name' => 'App commands notifier', 'group' => 'App', 'type' => 'command_notifier', 'source' => 'app_commands_notifier'],
                        ['name' => 'App queries handler', 'group' => 'App', 'type' => 'query_handler', 'queue' => 'app_queries', 'cmd_extra_options' => ['time_limit' => 3600]],
                    ],
                ],
                'result_storage' => [
                    'provider' => 'memory',
                ],
            ],
        ];
    }

    protected function configureExtraServices(ContainerConfigurator $container): void
    {
    }
}
