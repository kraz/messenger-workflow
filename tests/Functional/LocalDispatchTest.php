<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Functional;

use Kraz\MessengerWorkflow\Tests\Fixture\BusAccessor;
use Kraz\MessengerWorkflow\Tests\Fixture\Message\DuplicatedQuery;
use Kraz\MessengerWorkflow\Tests\Fixture\Message\FailingCommand;
use Kraz\MessengerWorkflow\Tests\Fixture\Message\TestCommand;
use Kraz\MessengerWorkflow\Tests\Fixture\Message\TestQuery;
use Kraz\MessengerWorkflow\Tests\Fixture\Message\UnhandledCommand;
use Kraz\MessengerWorkflow\Tests\Fixture\MessageRecorder;
use Kraz\MessengerWorkflow\Tests\Support\WorkflowKernelTestCase;
use Symfony\Component\Messenger\Exception\LogicException;

/**
 * Spec: Commands/Queries with a local handler execute synchronously in-process;
 * exactly-one-handler is enforced; handler exceptions surface unwrapped.
 */
final class LocalDispatchTest extends WorkflowKernelTestCase
{
    private BusAccessor $buses;
    private MessageRecorder $recorder;

    protected function setUp(): void
    {
        $container = self::getContainer();
        $this->buses = $container->get(BusAccessor::class);
        $this->recorder = $container->get(MessageRecorder::class);
        $this->recorder->reset();
    }

    public function testCommandWithLocalHandlerIsExecutedSynchronously(): void
    {
        $command = new TestCommand('sync-run');

        $this->buses->commandBus->dispatch($command);

        self::assertSame([$command], $this->recorder->ofType(TestCommand::class));
    }

    public function testCommandHandlerExceptionSurfacesUnwrapped(): void
    {
        $this->expectException(\DomainException::class);
        $this->expectExceptionMessage('kaboom');

        $this->buses->commandBus->dispatch(new FailingCommand('kaboom'));
    }

    public function testQueryReturnsHandlerResult(): void
    {
        $result = $this->buses->queryBus->ask(new TestQuery('abc'));

        self::assertSame('ABC', $result);
    }

    public function testQueryWithTwoHandlersIsRejected(): void
    {
        // Spec: "Queries must have exactly one handler"
        $this->expectException(LogicException::class);
        $this->expectExceptionMessageMatches('/handled multiple times/');

        $this->buses->queryBus->ask(new DuplicatedQuery());
    }

    public function testCommandWithoutLocalHandlerCannotRunSynchronously(): void
    {
        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessageMatches('/can not be executed synchronously/');

        $this->buses->commandBus->dispatch(new UnhandledCommand());
    }

    public function testWrongMessageTypeIsRejectedPerBus(): void
    {
        // Spec: type validation prevents wrong message type on wrong bus
        $query = new TestQuery();

        try {
            $this->buses->commandBus->dispatch($query);
            self::fail('Expected RuntimeException');
        } catch (\RuntimeException $e) {
            self::assertStringContainsString('Invalid command message', $e->getMessage());
        }

        try {
            $this->buses->eventBus->publish($query);
            self::fail('Expected RuntimeException');
        } catch (\RuntimeException $e) {
            self::assertStringContainsString('Invalid event message', $e->getMessage());
        }

        try {
            $this->buses->queryBus->ask(new TestCommand());
            self::fail('Expected RuntimeException');
        } catch (\RuntimeException $e) {
            self::assertStringContainsString('Invalid query message', $e->getMessage());
        }
    }

    public function testWrongMessageTypeIsRejectedByMiddlewareOnRawBuses(): void
    {
        $container = self::getContainer();

        try {
            $container->get('command.bus')->dispatch(new TestQuery());
            self::fail('Expected RuntimeException');
        } catch (\RuntimeException $e) {
            self::assertStringContainsString('Invalid command message', $e->getMessage());
        }

        try {
            $container->get('query.bus')->dispatch(new TestCommand());
            self::fail('Expected RuntimeException');
        } catch (\RuntimeException $e) {
            self::assertStringContainsString('Invalid query message', $e->getMessage());
        }

        try {
            $container->get('event.bus')->dispatch(new TestQuery());
            self::fail('Expected RuntimeException');
        } catch (\RuntimeException $e) {
            self::assertStringContainsString('Invalid event message', $e->getMessage());
        }
    }
}
