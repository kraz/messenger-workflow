<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Functional;

use Kraz\MessengerWorkflow\Application\Messenger\CommandBusInterface;
use Kraz\MessengerWorkflow\Application\Messenger\EventBusInterface;
use Kraz\MessengerWorkflow\Application\Messenger\QueryBusInterface;
use Kraz\MessengerWorkflow\Application\Task\ResultStorageInterface;
use Kraz\MessengerWorkflow\Tests\Support\WorkflowKernelTestCase;
use Kraz\MessengerWorkflow\Tests\TestKernel\BookAppDemo1BcKernel;
use Symfony\Bundle\FrameworkBundle\Console\Application;
use Symfony\Component\Console\Tester\CommandTester;

/**
 * Spec: config backward compatibility — the verbatim module YAML of the
 * BookAppDemo1 reference app must load and produce a valid container. The fixture
 * files under tests/Fixture/BookAppDemo1 are unmodified copies.
 */
final class BookAppDemo1ConfigBcTest extends WorkflowKernelTestCase
{
    protected static function getKernelClass(): string
    {
        return BookAppDemo1BcKernel::class;
    }

    public function testTheVerbatimReferenceConfigurationBoots(): void
    {
        self::bootKernel();
        $container = self::getContainer();

        self::assertInstanceOf(CommandBusInterface::class, $container->get(CommandBusInterface::class));
        self::assertInstanceOf(QueryBusInterface::class, $container->get(QueryBusInterface::class));
        self::assertInstanceOf(EventBusInterface::class, $container->get(EventBusInterface::class));
        self::assertInstanceOf(ResultStorageInterface::class, $container->get(ResultStorageInterface::class), 'The redis result storage wired against the snc_redis client');
    }

    public function testTheManualWorkerDeclarationsYieldExactlyTheDeclaredSet(): void
    {
        self::bootKernel();
        $workers = self::getContainer()->getParameter('messenger_workflow.workers');
        self::assertIsArray($workers);

        // 7 hand-written workers per module — the derivation must merge with the
        // manual entries by name/identity and produce no duplicates.
        self::assertCount(14, $workers);

        $names = array_map(static fn ($worker): string => \is_array($worker) && \is_string($worker['name'] ?? null) ? $worker['name'] : '', $workers);
        foreach ([
            'Book store events publisher',
            'Book store events receiver',
            'Book store events handler',
            'Book store commands receiver',
            'Book store commands handler',
            'Book store commands notifier',
            'Book store queries handler',
            'Book warehouse events publisher',
            'Book warehouse events receiver',
            'Book warehouse events handler',
            'Book warehouse commands receiver',
            'Book warehouse commands handler',
            'Book warehouse commands notifier',
            'Book warehouse queries handler',
        ] as $expected) {
            self::assertContains($expected, $names);
        }
    }

    public function testTheSupervisorConfigCommandRendersTheClassicOutput(): void
    {
        self::bootKernel();
        $kernel = self::$kernel;
        self::assertNotNull($kernel);
        $application = new Application($kernel);
        $application->setAutoExit(false);

        // CommandTester instead of Application::run — PHPUnit exports
        // SHELL_VERBOSITY=-1, which would silence the application output.
        $tester = new CommandTester($application->find('messenger:supervisor-config'));
        self::assertSame(0, $tester->execute([]));
        $display = $tester->getDisplay();

        self::assertStringContainsString('[group:book-store]', $display);
        self::assertStringContainsString('[group:book-warehouse]', $display);
        self::assertStringContainsString('[program:book-store-commands-handler]', $display);
        self::assertStringContainsString('[program:book-warehouse-events-publisher]', $display);
        self::assertStringContainsString('MSG_BROKER_CONN_NAME=', $display);
        self::assertStringContainsString('stdout_logfile=/dev/stdout', $display, 'worker_defaults supervisor overrides are applied');
    }
}
