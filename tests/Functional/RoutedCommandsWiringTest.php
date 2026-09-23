<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Functional;

use Contracts\Demo\Command\PlanSomethingCommand;
use Contracts\Demo\Command\ReplanSomethingCommand;
use Kraz\MessengerWorkflow\Infrastructure\Doctrine\Inbox\InboxTransport;
use Kraz\MessengerWorkflow\Infrastructure\Messenger\MessageRouteResolver;
use Kraz\MessengerWorkflow\Infrastructure\Messenger\Middleware\CommandNotifierMiddleware;
use Kraz\MessengerWorkflow\Tests\Support\WorkflowKernelTestCase;
use Kraz\MessengerWorkflow\Tests\TestKernel\RoutedCommandsKernel;
use Symfony\Bundle\FrameworkBundle\Console\Application;
use Symfony\Component\Console\Tester\CommandTester;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\DependencyInjection\Definition;
use Symfony\Component\DependencyInjection\Loader\Configurator\ContainerConfigurator;

/**
 * Spec: a routed commands queue compiles into the RoutedCommandsKernel container —
 * route map, binding keys, derived workers (no extra notifier), derived inbox table,
 * notifier resolution — while the regular queue of the context is untouched.
 */
final class RoutedCommandsWiringTest extends WorkflowKernelTestCase
{
    protected static function getKernelClass(): string
    {
        return RoutedCommandsKernel::class;
    }

    public function testTheRouteMapContainsTheListedAndTheAttributedClass(): void
    {
        self::bootKernel();
        $resolver = self::getContainer()->get('messenger_workflow.message_route_resolver');
        self::assertInstanceOf(MessageRouteResolver::class, $resolver);

        self::assertSame(
            [PlanSomethingCommand::class => 'planning', ReplanSomethingCommand::class => 'planning'],
            $resolver->getRoutes(),
        );
    }

    public function testTheDerivedWorkersAddAReceiverAndAHandlerInTheContextGroupAndNoNotifier(): void
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

        // The TestKernel's 12 workers + receiver + handler for app_planning.
        self::assertCount(14, $byName);
        self::assertSame('app', $byName['app_planning receiver']['group'] ?? null);
        self::assertSame('app', $byName['app_planning handler']['group'] ?? null);
        self::assertSame('app_planning', $byName['app_planning handler']['source'] ?? null);
        self::assertArrayNotHasKey('app_planning_notifier', $byName);
        self::assertArrayHasKey('app_commands_notifier', $byName, 'The context keeps exactly one notifier worker');
    }

    public function testTheSupervisorConfigRendersTheRoutedWorkersAtOneProcess(): void
    {
        self::bootKernel();
        $kernel = self::$kernel;
        self::assertNotNull($kernel);
        $application = new Application($kernel);
        $application->setAutoExit(false);

        $tester = new CommandTester($application->find('messenger:supervisor-config'));
        self::assertSame(0, $tester->execute([]));
        $display = $tester->getDisplay();

        self::assertStringContainsString('command=bin/console messenger:consume --bus=inbox.bus --queues=app_planning --sleep=0 commands', $display);
        self::assertStringContainsString('[program:app-planning-handler]', $display);
        self::assertStringContainsString('command=bin/console messenger:consume --bus=command.bus app_planning', $display);
        self::assertSame(1, substr_count($display, '--bus=notifier.bus'), 'Exactly one notifier worker for the context');
    }

    public function testTheRoutedInboxGotItsOwnDerivedTable(): void
    {
        self::bootKernel();
        $planning = self::getContainer()->get('messenger.transport.app_planning');
        self::assertInstanceOf(InboxTransport::class, $planning);
        $regular = self::getContainer()->get('messenger.transport.app_commands');
        self::assertInstanceOf(InboxTransport::class, $regular);

        self::assertSame('zz_commands_inbox_app_planning', $planning->getConnection()->getTableName());
        self::assertSame('zz_commands_inbox', $regular->getConnection()->getTableName());
        self::assertTrue($planning->getConnection()->isStrictOrder());
    }

    public function testTheRoutedQueueResolvesTheContextsNotifier(): void
    {
        self::bootKernel();
        $middleware = self::getContainer()->get('messenger_workflow.command_notifier_middleware');
        self::assertInstanceOf(CommandNotifierMiddleware::class, $middleware);

        $map = new \ReflectionProperty(CommandNotifierMiddleware::class, 'notifierByQueue')->getValue($middleware);
        self::assertSame(['app_planning' => 'app_commands_notifier'], $map);
    }

    public function testTheBindingKeysOfTheRoutedAndTheRegularQueue(): void
    {
        // The compiled container drops the definition; rebuild the relevant part the
        // way the kernel does and inspect the transport arguments before compilation.
        $kernel = new RoutedCommandsKernel('test', false);
        $kernel->boot();
        $container = new \ReflectionMethod($kernel, 'buildContainer')->invoke($kernel);
        self::assertInstanceOf(ContainerBuilder::class, $container);
        $container->getCompilerPassConfig()->setRemovingPasses([]);
        $container->compile();

        $definition = $container->getDefinition('messenger.transport.commands');
        self::assertInstanceOf(Definition::class, $definition);
        $options = $definition->getArgument(1);
        self::assertIsArray($options);
        $queues = $options['queues'] ?? null;
        self::assertIsArray($queues);

        $planning = $queues['app_planning'] ?? null;
        $regular = $queues['app_commands'] ?? null;
        self::assertIsArray($planning);
        self::assertIsArray($regular);
        self::assertSame(['commands.Demo.planning', 'commands.internal.Demo.planning'], $planning['binding_keys'] ?? null);
        self::assertSame(['commands.Demo', 'commands.internal.Demo'], $regular['binding_keys'] ?? null);
        $kernel->shutdown();
    }
}
