<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Functional;

use Kraz\MessengerWorkflow\MessengerWorkflowBundle;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;
use Symfony\Component\Config\Definition\Processor;
use Symfony\Component\DependencyInjection\ContainerBuilder;

/**
 * Spec: "7 worker types: event_publisher, event_receiver, event_handler, command_receiver,
 * command_handler, command_notifier, query_handler" — validates the per-type defaults and
 * required options of the bundle's workflow worker configuration.
 */
final class WorkerConfigNormalizationTest extends TestCase
{
    private function process(array $workers, array $workerDefaults = []): array
    {
        $bundle = new MessengerWorkflowBundle();
        $extension = $bundle->getContainerExtension();
        $container = new ContainerBuilder();
        $configuration = $extension->getConfiguration([], $container);
        self::assertNotNull($configuration);

        $config = (new Processor())->processConfiguration($configuration, [
            'messenger_workflow' => [
                'messenger' => [
                    'workflow' => array_filter([
                        'worker_defaults' => $workerDefaults,
                        'workers' => $workers,
                    ]),
                ],
            ],
        ]);

        return $config['messenger']['workflow']['workers'] ?? [];
    }

    public function testEventPublisherDefaultsTargetToEventBus(): void
    {
        [$worker] = $this->process([
            ['name' => 'P', 'group' => 'G', 'type' => 'event_publisher', 'source' => 'app_outbox'],
        ]);

        self::assertSame('event.bus', $worker['target']);
        self::assertSame('app_outbox', $worker['source']);
        self::assertSame(1, $worker['instances']);
    }

    public function testEventReceiverDefaultsSourceTargetAndSleep(): void
    {
        [$worker] = $this->process([
            ['name' => 'R', 'group' => 'G', 'type' => 'event_receiver', 'queue' => 'app_events'],
        ]);

        self::assertSame('events', $worker['source']);
        self::assertSame('inbox.bus', $worker['target']);
        self::assertSame(0.0, $worker['cmd_extra_options']['sleep']);
    }

    public function testCommandWorkersDefaults(): void
    {
        [$receiver, $handler, $notifier] = $this->process([
            ['name' => 'R', 'group' => 'G', 'type' => 'command_receiver', 'queue' => 'app_commands'],
            ['name' => 'H', 'group' => 'G', 'type' => 'command_handler', 'source' => 'app_commands'],
            ['name' => 'N', 'group' => 'G', 'type' => 'command_notifier', 'source' => 'app_commands_notifier'],
        ]);

        self::assertSame('commands', $receiver['source']);
        self::assertSame('inbox.bus', $receiver['target']);

        self::assertSame('command.bus', $handler['target']);

        self::assertSame('outbox.bus', $notifier['target']);
    }

    public function testQueryHandlerDefaults(): void
    {
        [$worker] = $this->process([
            ['name' => 'Q', 'group' => 'G', 'type' => 'query_handler', 'queue' => 'app_queries'],
        ]);

        self::assertSame('queries', $worker['source']);
        self::assertSame('query.bus', $worker['target']);
        self::assertSame(0.0, $worker['cmd_extra_options']['sleep']);
    }

    public function testEventPublisherRequiresSource(): void
    {
        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessageMatches('/must have a "source" transport/');

        $this->process([
            ['name' => 'P', 'group' => 'G', 'type' => 'event_publisher'],
        ]);
    }

    public function testReceiversRequireAQueue(): void
    {
        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessageMatches('/must have a "queue" set/');

        $this->process([
            ['name' => 'R', 'group' => 'G', 'type' => 'event_receiver'],
        ]);
    }

    public function testUnknownWorkerTypeIsRejected(): void
    {
        $this->expectException(\LogicException::class);
        $this->expectExceptionMessageMatches('/Unknown worker type/');

        $this->process([
            ['name' => 'X', 'group' => 'G', 'type' => 'nonsense'],
        ]);
    }
}
