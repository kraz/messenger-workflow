<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\TestKernel;

use Kraz\MessengerWorkflow\MessengerWorkflowBundle;
use Kraz\MessengerWorkflow\Tests\Fixture\Handler\BetaEventHandler;
use Kraz\MessengerWorkflow\Tests\Fixture\Handler\ContractEventFromTransportHandler;
use Kraz\MessengerWorkflow\Tests\Fixture\Handler\DoSomethingCommandHandler;
use Kraz\MessengerWorkflow\Tests\Fixture\Handler\DuplicatedQueryHandlers;
use Kraz\MessengerWorkflow\Tests\Fixture\Handler\ExtraServicesCommandController;
use Kraz\MessengerWorkflow\Tests\Fixture\Handler\ExtraServicesEventHandlers;
use Kraz\MessengerWorkflow\Tests\Fixture\Handler\MoreExtraServicesEventHandlers;
use Kraz\MessengerWorkflow\Tests\Fixture\Handler\GammaTransactionalEventHandler;
use Kraz\MessengerWorkflow\Tests\Fixture\Handler\GetSomethingQueryHandler;
use Kraz\MessengerWorkflow\Tests\Fixture\Handler\OrderedEventsHandler;
use Kraz\MessengerWorkflow\Tests\Fixture\Retry\ForcedRetryDecider;
use Kraz\MessengerWorkflow\Tests\Fixture\Handler\FromTransportEventHandler;
use Kraz\MessengerWorkflow\Tests\Fixture\Handler\TestCommandHandler;
use Kraz\MessengerWorkflow\Tests\Fixture\Handler\TestEventHandlers;
use Kraz\MessengerWorkflow\Tests\Fixture\Handler\TestQueryHandler;
use Kraz\MessengerWorkflow\Tests\Fixture\MessageRecorder;
use Doctrine\Bundle\DoctrineBundle\DoctrineBundle;
use Jwage\PhpAmqpLibMessengerBundle\PhpAmqpLibMessengerBundle;
use Kraz\MessengerWorkflow\Tests\Support\ExposeHandlersLocatorsPass;
use Symfony\Bundle\FrameworkBundle\FrameworkBundle;
use Symfony\Bundle\FrameworkBundle\Kernel\MicroKernelTrait;
use Symfony\Component\DependencyInjection\Compiler\PassConfig;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\DependencyInjection\Loader\Configurator\ContainerConfigurator;
use Symfony\Component\HttpKernel\Kernel;

use function Symfony\Component\DependencyInjection\Loader\Configurator\service;

class TestKernel extends Kernel
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

    protected function build(ContainerBuilder $container): void
    {
        $container->addCompilerPass(new ExposeHandlersLocatorsPass(), PassConfig::TYPE_BEFORE_REMOVING);
    }

    protected function configureContainer(ContainerConfigurator $container): void
    {
        $container->extension('framework', [
            'secret' => 'test',
            'test' => true,
            'http_method_override' => false,
            'php_errors' => ['log' => true],
            // Buses, routing and transport options are prepended by the bundle;
            // the application only supplies the broker DSNs.
            'messenger' => [
                'transports' => [
                    'commands' => ['dsn' => '%env(MWF_TEST_AMQP_DSN)%'],
                    'queries' => ['dsn' => '%env(MWF_TEST_AMQP_DSN)%'],
                    'events' => ['dsn' => '%env(MWF_TEST_AMQP_DSN)%'],
                    // Short notify timeouts: a NOTIFY sent before the consumer LISTENs is
                    // missed; the re-poll interval caps that window (60s default).
                    'app_outbox' => ['dsn' => 'events-outbox://postgres?get_notify_timeout=250&check_delayed_interval=250'],
                    'app_commands_outbox' => ['dsn' => 'commands-outbox://postgres?get_notify_timeout=250&check_delayed_interval=250'],
                    // Notifier outbox for the app_commands receiver (convention:
                    // "<receiver>_notifier"); needs its own table to not share the
                    // publisher outbox's default one.
                    'app_commands_notifier' => ['dsn' => 'commands-outbox://postgres?table_name=zz_commands_notifier&get_notify_timeout=250&check_delayed_interval=250'],
                    // Inbox transports — by convention named after the broker queue they receive from.
                    'app_commands' => [
                        'dsn' => 'commands-inbox://postgres?get_notify_timeout=250&check_delayed_interval=250',
                        'failure_transport' => 'app_commands_failures',
                    ],
                    'app_commands_failures' => ['dsn' => 'commands-failures://postgres?queue_name=app_commands'],
                    'app_events' => [
                        'dsn' => 'events-inbox://postgres?get_notify_timeout=250&check_delayed_interval=250',
                        'failure_transport' => 'app_events_failures',
                    ],
                    'app_events_failures' => ['dsn' => 'events-failures://postgres?queue_name=app_events'],
                    // Second consumer context on its OWN database (sqlite) — multi-DB fan-out.
                    'beta_events' => [
                        'dsn' => 'events-inbox://default',
                        'failure_transport' => 'beta_events_failures',
                    ],
                    'beta_events_failures' => ['dsn' => 'events-failures://default?queue_name=beta_events'],
                    // Third consumer context: transactional events inbox (optional mode).
                    'gamma_events' => [
                        'dsn' => 'events-inbox://postgres?table_name=zz_gamma_inbox&transactional_handler=true&get_notify_timeout=250&check_delayed_interval=250',
                        'failure_transport' => 'gamma_events_failures',
                    ],
                    'gamma_events_failures' => ['dsn' => 'events-failures://postgres?queue_name=gamma_events'],
                ],
            ],
        ]);

        $pgEnv = static fn (string $name, string $default): string => \is_string($_ENV[$name] ?? null) && '' !== $_ENV[$name] ? $_ENV[$name] : $default;
        $container->extension('doctrine', [
            'dbal' => [
                'default_connection' => 'default',
                'connections' => [
                    'default' => [
                        'driver' => 'pdo_sqlite',
                        'path' => MWF_TEST_VAR_DIR.'/db-'.hash('xxh64', static::class).'.sqlite',
                        'server_version' => '3.45.0',
                    ],
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
                    // Owners are chosen to match the fixture namespaces: internal fixture
                    // messages live under Kraz\..., contract fixtures under Contracts\Demo\...
                    'events' => [
                        'queue_bindings' => [
                            ['queue' => 'app_events', 'owner' => 'Kraz', 'binding_keys' => ['Contracts\Demo\Event\SomethingHappened']],
                            // beta/gamma binding keys are auto-derived from the fixture
                            // handlers' fromTransport attributes.
                            ['queue' => 'beta_events', 'owner' => 'Beta'],
                            ['queue' => 'gamma_events', 'owner' => 'Gamma'],
                        ],
                    ],
                    'commands' => [
                        'queue_bindings' => [
                            ['queue' => 'app_commands', 'owner' => 'Demo'],
                        ],
                    ],
                    'queries' => [
                        'queue_bindings' => [
                            ['queue' => 'app_queries', 'owner' => 'Demo'],
                        ],
                    ],
                ],
                'outbox_buses' => [
                    'app' => 'app_outbox',
                ],
            ],
        ]);

        $this->registerFixtureServices($container);
    }

    protected function registerFixtureServices(ContainerConfigurator $container): void
    {
        $services = $container->services()->defaults()->autowire()->autoconfigure();

        $services->set(MessageRecorder::class)->public();
        $services->set(TestCommandHandler::class);
        $services->set(DoSomethingCommandHandler::class);
        $services->set(ForcedRetryDecider::class);
        $services->set(TestQueryHandler::class);
        $services->set(TestEventHandlers::class);
        $services->set(DuplicatedQueryHandlers::class);
        $services->set(ExtraServicesCommandController::class);
        $services->set(ExtraServicesEventHandlers::class);
        $services->set(MoreExtraServicesEventHandlers::class);
        $services->set(FromTransportEventHandler::class);
        $services->set(ContractEventFromTransportHandler::class);
        $services->set(BetaEventHandler::class);
        $services->set(OrderedEventsHandler::class)
            ->arg('$postgres', service('doctrine.dbal.postgres_connection'));
        $services->set(GammaTransactionalEventHandler::class)
            ->arg('$postgres', service('doctrine.dbal.postgres_connection'));
        $services->set(GetSomethingQueryHandler::class);
    }
}
