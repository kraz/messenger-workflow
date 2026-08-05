<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Functional;

use Kraz\MessengerWorkflow\Application\Messenger\CommandBusInterface;
use Kraz\MessengerWorkflow\Infrastructure\Messenger\CommandBus;
use Kraz\MessengerWorkflow\Infrastructure\Messenger\Middleware\CommandNotifierMiddleware;
use Kraz\MessengerWorkflow\Infrastructure\Messenger\Middleware\ExactlyOneHandlerMiddleware;
use Kraz\MessengerWorkflow\Infrastructure\Messenger\Middleware\ResultNotifierMiddleware;
use Kraz\MessengerWorkflow\Infrastructure\Messenger\Retry\RetryDecidingStrategy;
use Kraz\MessengerWorkflow\Tests\Support\WorkflowKernelTestCase;
use Symfony\Component\Messenger\MessageBusInterface;

/**
 * Command-flow container wiring: the CommandBus service behind CommandBusInterface,
 * the notifier bus and middlewares, and the workflow retry strategy applied to the
 * commands-inbox transport.
 */
final class CommandFlowWiringTest extends WorkflowKernelTestCase
{
    public function testCommandBusInterfaceResolvesToTheWorkflowCommandBus(): void
    {
        self::assertInstanceOf(CommandBus::class, self::getContainer()->get(CommandBusInterface::class));
    }

    public function testTheNotifierBusAndItsMiddlewareAreRegistered(): void
    {
        $container = self::getContainer();

        self::assertInstanceOf(MessageBusInterface::class, $container->get('notifier.bus'));
        self::assertInstanceOf(ResultNotifierMiddleware::class, $container->get('messenger_workflow.result_notifier_middleware'));
        self::assertInstanceOf(CommandNotifierMiddleware::class, $container->get('messenger_workflow.command_notifier_middleware'));
        self::assertInstanceOf(ExactlyOneHandlerMiddleware::class, $container->get('messenger_workflow.exactly_one_handler_middleware'));
    }

    public function testTheInboxTransportsUseTheWorkflowRetryStrategies(): void
    {
        $strategyLocator = self::getContainer()->get('messenger.retry_strategy_locator');
        self::assertInstanceOf(\Psr\Container\ContainerInterface::class, $strategyLocator);

        self::assertInstanceOf(RetryDecidingStrategy::class, $strategyLocator->get('app_commands'));
        self::assertInstanceOf(RetryDecidingStrategy::class, $strategyLocator->get('app_events'));
        self::assertNotSame($strategyLocator->get('app_commands'), $strategyLocator->get('app_events'), 'Commands and events have distinct retry policies');
    }
}
