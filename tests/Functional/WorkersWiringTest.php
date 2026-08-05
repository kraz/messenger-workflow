<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Functional;

use Kraz\MessengerWorkflow\Tests\Support\WorkflowKernelTestCase;
use Symfony\Bundle\FrameworkBundle\Console\Application;
use Symfony\Component\Console\Tester\CommandTester;

/**
 * Worker derivation: the worker set is auto-derived from the TestKernel's transport
 * topology and exposed to the messenger:supervisor-config command.
 */
final class WorkersWiringTest extends WorkflowKernelTestCase
{
    public function testTheWorkerSetIsDerivedFromTheTransportTopology(): void
    {
        self::bootKernel();
        $workers = self::getContainer()->getParameter('messenger_workflow.workers');
        self::assertIsArray($workers);

        $byName = [];
        foreach ($workers as $worker) {
            self::assertIsArray($worker);
            self::assertIsString($worker['name'] ?? null);
            $byName[$worker['name']] = $worker;
        }

        // 3 outbox transports (2 publishers + 1 notifier), 4 inbox queues ×
        // (receiver + handler), 1 query handler.
        self::assertCount(12, $byName);
        self::assertSame('relay.bus', $byName['app_outbox publisher']['target'] ?? null);
        self::assertSame('relay.bus', $byName['app_commands_outbox publisher']['target'] ?? null);
        self::assertSame('notifier.bus', $byName['app_commands_notifier']['target'] ?? null);
        self::assertSame('inbox.bus', $byName['app_commands receiver']['target'] ?? null);
        self::assertSame('command.bus', $byName['app_commands handler']['target'] ?? null);
        self::assertSame('event.bus', $byName['beta_events handler']['target'] ?? null);
        self::assertSame('event.bus', $byName['gamma_events handler']['target'] ?? null);
        self::assertSame('query.bus', $byName['app_queries handler']['target'] ?? null);
    }

    public function testTheSupervisorConfigCommandRendersTheDerivedWorkers(): void
    {
        self::bootKernel();
        $kernel = self::$kernel;
        self::assertNotNull($kernel);
        $application = new Application($kernel);
        $application->setAutoExit(false);

        $tester = new CommandTester($application->find('messenger:supervisor-config'));
        self::assertSame(0, $tester->execute([]));
        $display = $tester->getDisplay();

        self::assertStringContainsString('[group:app]', $display);
        self::assertStringContainsString('command=bin/console messenger:consume --bus=relay.bus app_outbox', $display);
        self::assertStringContainsString('command=bin/console messenger:consume --bus=notifier.bus app_commands_notifier', $display);
        self::assertStringContainsString('command=bin/console messenger:consume --bus=inbox.bus --queues=app_commands --sleep=0 commands', $display);
        self::assertStringContainsString('command=bin/console messenger:consume --bus=query.bus --queues=app_queries --sleep=0 queries', $display);
        self::assertStringContainsString('MSG_BROKER_CONN_NAME=', $display);
    }
}
