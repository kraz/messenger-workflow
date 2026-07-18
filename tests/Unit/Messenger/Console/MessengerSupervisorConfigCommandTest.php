<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Unit\Messenger\Console;

use Kraz\MessengerWorkflow\Messenger\Console\MessengerSupervisorConfigCommand;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Console\Tester\CommandTester;
use Symfony\Component\DependencyInjection\Container;
use Symfony\Component\DependencyInjection\ParameterBag\ContainerBag;
use Symfony\Component\DependencyInjection\ParameterBag\ParameterBag;

final class MessengerSupervisorConfigCommandTest extends TestCase
{
    private function execute(array $workers, array $defaults = []): string
    {
        $container = new Container(new ParameterBag(['kernel.project_dir' => '/srv/app']));
        $command = new MessengerSupervisorConfigCommand($workers, $defaults, new ContainerBag($container));
        $tester = new CommandTester($command);
        $tester->execute([]);
        $tester->assertCommandIsSuccessful();

        return $tester->getDisplay();
    }

    public function testCommandName(): void
    {
        // Spec/README lists "mwf:supervisor:config" — actual implementation uses this name:
        $container = new Container(new ParameterBag(['kernel.project_dir' => '/srv/app']));
        $command = new MessengerSupervisorConfigCommand([], [], new ContainerBag($container));

        self::assertSame('messenger:supervisor-config', $command->getName());
    }

    public function testGeneratesGroupAndProgramSections(): void
    {
        $output = $this->execute([
            [
                'name' => 'App events publisher',
                'group' => 'App',
                'type' => 'event_publisher',
                'source' => 'app_outbox',
                'target' => 'event.bus',
                'instances' => 1,
            ],
            [
                'name' => 'App commands receiver',
                'group' => 'App',
                'type' => 'command_receiver',
                'source' => 'commands',
                'target' => 'inbox.bus',
                'queue' => 'app_commands',
                'instances' => 2,
            ],
        ]);

        self::assertStringContainsString('[group:app]', $output);
        self::assertStringContainsString('programs=app-events-publisher,app-commands-receiver', $output);

        self::assertStringContainsString('[program:app-events-publisher]', $output);
        self::assertStringContainsString('command=bin/console messenger:consume --bus=event.bus app_outbox', $output);
        self::assertStringContainsString('directory=/srv/app', $output);
        self::assertStringContainsString('process_name=%(program_name)s'.\PHP_EOL, $output);

        self::assertStringContainsString('[program:app-commands-receiver]', $output);
        self::assertStringContainsString(
            'command=bin/console messenger:consume --bus=inbox.bus --queues=app_commands commands',
            $output,
        );
        self::assertStringContainsString('numprocs=2', $output);
        self::assertStringContainsString('process_name=%(program_name)s-%(process_num)02d', $output);
        self::assertStringContainsString('environment=MSG_BROKER_CONN_NAME="app-commands-receiver"', $output);
    }

    public function testExtraConsumeOptionsAreRendered(): void
    {
        $output = $this->execute([
            [
                'name' => 'Q handler',
                'group' => 'App',
                'type' => 'query_handler',
                'source' => 'queries',
                'target' => 'query.bus',
                'queue' => 'app_queries',
                'instances' => 1,
                'cmd_extra_options' => [
                    'limit' => 100,
                    'memory_limit' => 128,
                    'time_limit' => 3600,
                    'sleep' => 0.0,
                    'verbose' => '-vv',
                ],
            ],
        ]);

        self::assertStringContainsString('--limit=100', $output);
        self::assertStringContainsString('--memory-limit=128', $output);
        self::assertStringContainsString('--time-limit=3600', $output);
        self::assertStringContainsString('--sleep=0', $output);
        self::assertStringContainsString('-vv', $output);
    }

    public function testSupervisorDefaultsAreMergedIntoPrograms(): void
    {
        $output = $this->execute(
            [[
                'name' => 'W one',
                'group' => 'App',
                'type' => 'event_publisher',
                'source' => 'app_outbox',
                'target' => 'event.bus',
                'instances' => 1,
            ]],
            ['supervisor' => ['autostart' => 'true', 'stopwaitsecs' => 20]],
        );

        self::assertStringContainsString('autostart=true', $output);
        self::assertStringContainsString('stopwaitsecs=20', $output);
    }

    public function testDuplicateProgramNameInGroupIsRejected(): void
    {
        $worker = [
            'name' => 'Same name',
            'group' => 'App',
            'type' => 'event_publisher',
            'source' => 'app_outbox',
            'target' => 'event.bus',
            'instances' => 1,
        ];

        $this->expectException(\LogicException::class);
        $this->expectExceptionMessageMatches('/already defined/');

        $this->execute([$worker, $worker]);
    }
}
