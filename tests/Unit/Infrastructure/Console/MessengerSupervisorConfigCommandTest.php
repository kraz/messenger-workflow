<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Unit\Infrastructure\Console;

use Kraz\MessengerWorkflow\Infrastructure\Console\MessengerSupervisorConfigCommand;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Console\Tester\CommandTester;
use Symfony\Component\DependencyInjection\Container;
use Symfony\Component\DependencyInjection\ParameterBag\ContainerBag;
use Symfony\Component\DependencyInjection\ParameterBag\ParameterBag;

/**
 * Golden-file test: the output shape must match the original package's command —
 * [group:x]/[program:y] blocks, numprocs, MSG_BROKER_CONN_NAME env and supervisor
 * override merging.
 */
final class MessengerSupervisorConfigCommandTest extends TestCase
{
    /**
     * @param list<array<array-key, mixed>> $workers
     * @param array<array-key, mixed>       $defaults
     */
    private function command(array $workers, array $defaults = []): MessengerSupervisorConfigCommand
    {
        return new MessengerSupervisorConfigCommand(
            $workers,
            $defaults,
            new ContainerBag(new Container(new ParameterBag(['kernel.project_dir' => '/app']))),
        );
    }

    public function testGoldenOutputForATypicalWorkerSet(): void
    {
        $tester = new CommandTester($this->command([
            [
                'name' => 'Book store events publisher',
                'group' => 'Book store',
                'type' => 'event_publisher',
                'source' => 'book_store_outbox',
                'target' => 'relay.bus',
                'instances' => 1,
            ],
            [
                'name' => 'Book store commands receiver',
                'group' => 'Book store',
                'type' => 'command_receiver',
                'source' => 'commands',
                'queue' => 'book_store_commands',
                'target' => 'inbox.bus',
                'instances' => 2,
                'cmd_extra_options' => ['sleep' => 0.0],
            ],
            [
                'name' => 'Book store commands handler',
                'group' => 'Book store',
                'type' => 'command_handler',
                'source' => 'book_store_commands',
                'target' => 'command.bus',
                'instances' => 1,
                'cmd_extra_options' => ['time_limit' => 3600, 'memory_limit' => '128M'],
                'supervisor' => ['autostart' => 'true', 'environment' => 'APP_ENV="prod"'],
            ],
        ]));

        self::assertSame(0, $tester->execute([]));

        $expected = <<<'CONF'
            [group:book-store]
            programs=book-store-events-publisher,book-store-commands-receiver,book-store-commands-handler

            [program:book-store-events-publisher]
            command=bin/console messenger:consume --bus=relay.bus book_store_outbox
            environment=MSG_BROKER_CONN_NAME="book-store-events-publisher"
            directory=/app
            numprocs=1
            numprocs_start=1
            process_name=%(program_name)s

            [program:book-store-commands-receiver]
            command=bin/console messenger:consume --bus=inbox.bus --queues=book_store_commands --sleep=0 commands
            environment=MSG_BROKER_CONN_NAME="book-store-commands-receiver"
            directory=/app
            numprocs=2
            numprocs_start=1
            process_name=%(program_name)s-%(process_num)02d

            [program:book-store-commands-handler]
            command=bin/console messenger:consume --bus=command.bus --memory-limit=128M --time-limit=3600 book_store_commands
            environment=APP_ENV="prod",MSG_BROKER_CONN_NAME="book-store-commands-handler"
            directory=/app
            numprocs=1
            numprocs_start=1
            process_name=%(program_name)s
            autostart=true

            CONF;

        self::assertSame($expected."\n", $tester->getDisplay());
    }

    public function testWorkerDefaultsAreMergedIntoEveryProgram(): void
    {
        $tester = new CommandTester($this->command(
            [[
                'name' => 'Relay',
                'group' => 'Ops',
                'source' => 'app_outbox',
                'target' => 'relay.bus',
            ]],
            ['instances' => 3, 'cmd_extra_options' => ['time_limit' => 600]],
        ));

        self::assertSame(0, $tester->execute([]));
        $display = $tester->getDisplay();

        self::assertStringContainsString('numprocs=3', $display);
        self::assertStringContainsString('--time-limit=600', $display);
        self::assertStringContainsString('process_name=%(program_name)s-%(process_num)02d', $display);
    }

    public function testKeepaliveIsRenderedIntoTheConsumeCommand(): void
    {
        $tester = new CommandTester($this->command([
            [
                'name' => 'App commands handler',
                'group' => 'App',
                'type' => 'command_handler',
                'source' => 'app_commands',
                'target' => 'command.bus',
                'cmd_extra_options' => ['keepalive' => 60],
            ],
        ]));

        self::assertSame(0, $tester->execute([]));

        self::assertStringContainsString(
            'command=bin/console messenger:consume --bus=command.bus --keepalive=60 app_commands',
            $tester->getDisplay(),
        );
    }

    public function testDuplicateProgramNamesAreRejected(): void
    {
        $tester = new CommandTester($this->command([
            ['name' => 'Same name', 'group' => 'G', 'source' => 'a', 'target' => 'relay.bus'],
            ['name' => 'Same name', 'group' => 'G', 'source' => 'b', 'target' => 'relay.bus'],
        ]));

        $this->expectException(\LogicException::class);
        $this->expectExceptionMessageMatches('/already defined for group/');

        $tester->execute([]);
    }

    public function testOutputDirWritesOneFilePerGroup(): void
    {
        $dir = sys_get_temp_dir().'/mwf-supervisor-'.bin2hex(random_bytes(4));

        try {
            $tester = new CommandTester($this->command([
                ['name' => 'A relay', 'group' => 'Alpha', 'source' => 'a_outbox', 'target' => 'relay.bus'],
                ['name' => 'B relay', 'group' => 'Beta', 'source' => 'b_outbox', 'target' => 'relay.bus'],
            ]));

            self::assertSame(0, $tester->execute(['--output-dir' => $dir]));

            self::assertFileExists($dir.'/alpha.conf');
            self::assertFileExists($dir.'/beta.conf');
            $alpha = (string) file_get_contents($dir.'/alpha.conf');
            self::assertStringContainsString('[group:alpha]', $alpha);
            self::assertStringContainsString('command=bin/console messenger:consume --bus=relay.bus a_outbox', $alpha);
        } finally {
            $leftovers = glob($dir.'/*.conf');
            foreach (false !== $leftovers ? $leftovers : [] as $file) {
                unlink($file);
            }
            if (is_dir($dir)) {
                rmdir($dir);
            }
        }
    }
}
