<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Unit\Infrastructure\Task;

use Contracts\Demo\Command\DoSomethingCommand;
use Contracts\Demo\Query\GetSomethingQuery;
use Kraz\MessengerWorkflow\Application\Messenger\CommandBusInterface;
use Kraz\MessengerWorkflow\Application\Messenger\QueryBusInterface;
use Kraz\MessengerWorkflow\Application\Task\TaskOwnership;
use Kraz\MessengerWorkflow\Application\Task\TaskOwnershipRegistryInterface;
use Kraz\MessengerWorkflow\Application\Task\TaskOwnerResolverInterface;
use Kraz\MessengerWorkflow\Infrastructure\Task\TrackingCommandBus;
use Kraz\MessengerWorkflow\Infrastructure\Task\TrackingQueryBus;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Messenger\Envelope;

final class TrackingBusesTest extends TestCase
{
    /**
     * @var TaskOwnershipRegistryInterface&object{records: array<string, array{string, ?string}>}
     */
    private TaskOwnershipRegistryInterface $registry;

    protected function setUp(): void
    {
        $this->registry = new class implements TaskOwnershipRegistryInterface {
            /**
             * @var array<string, array{string, ?string}>
             */
            public array $records = [];

            public function record(string $taskId, string $userIdentifier, ?string $messageType = null): void
            {
                $this->records[$taskId] = [$userIdentifier, $messageType];
            }

            public function find(string $taskId): ?TaskOwnership
            {
                return null;
            }
        };
    }

    private function resolver(?string $owner): TaskOwnerResolverInterface
    {
        return new class($owner) implements TaskOwnerResolverInterface {
            public function __construct(private readonly ?string $owner)
            {
            }

            public function resolveOwnerIdentifier(): ?string
            {
                return $this->owner;
            }
        };
    }

    /**
     * @return CommandBusInterface&object{trackedCalls: list<bool>}
     */
    private function innerCommandBus(): CommandBusInterface
    {
        return new class implements CommandBusInterface {
            /**
             * @var list<bool>
             */
            public array $trackedCalls = [];

            public function dispatch(object $command, ?string &$taskId = null): void
            {
                $tracked = \func_num_args() >= 2;
                $this->trackedCalls[] = $tracked;
                if ($tracked) {
                    $taskId = 'task-'.\count($this->trackedCalls);
                }
            }

            public function await(string $taskId, ?int $timeout = null): void
            {
            }
        };
    }

    public function testTrackedDispatchWithAnOwnerIsRecorded(): void
    {
        $inner = $this->innerCommandBus();
        $bus = new TrackingCommandBus($inner, $this->registry, $this->resolver('user-1'));

        $taskId = null;
        $bus->dispatch(new DoSomethingCommand('p'), $taskId);

        self::assertSame('task-1', $taskId);
        self::assertSame(['user-1', DoSomethingCommand::class], $this->registry->records['task-1']);
    }

    public function testUntrackedDispatchForwardsArgumentAbsenceAndRecordsNothing(): void
    {
        $inner = $this->innerCommandBus();
        $bus = new TrackingCommandBus($inner, $this->registry, $this->resolver('user-1'));

        $bus->dispatch(new DoSomethingCommand('p'));

        self::assertSame([false], $inner->trackedCalls, 'The decorator must forward argument PRESENCE, not a null value');
        self::assertSame([], $this->registry->records);
    }

    public function testOwnerlessDispatchesAreSystemTasksAndNotRecorded(): void
    {
        $bus = new TrackingCommandBus($this->innerCommandBus(), $this->registry, $this->resolver(null));
        $taskId = null;
        $bus->dispatch(new DoSomethingCommand('p'), $taskId);
        self::assertNotNull($taskId);
        self::assertSame([], $this->registry->records);

        $withoutResolver = new TrackingCommandBus($this->innerCommandBus(), $this->registry);
        $withoutResolver->dispatch(new DoSomethingCommand('p'), $taskId);
        self::assertSame([], $this->registry->records);
    }

    public function testEnvelopeDispatchRecordsTheInnerMessageType(): void
    {
        $bus = new TrackingCommandBus($this->innerCommandBus(), $this->registry, $this->resolver('user-1'));

        $taskId = null;
        $bus->dispatch(new Envelope(new DoSomethingCommand('p')), $taskId);

        self::assertNotNull($taskId);
        self::assertSame(DoSomethingCommand::class, $this->registry->records[$taskId][1]);
    }

    public function testQueryAskAsyncRecordsOwnership(): void
    {
        $inner = new class implements QueryBusInterface {
            public function ask(object $query, ?int $timeout = null): mixed
            {
                return 'sync-answer';
            }

            public function askAsync(object $query): string
            {
                return 'task-q1';
            }

            public function await(string $taskId, ?int $timeout = null): mixed
            {
                return 'awaited';
            }
        };
        $bus = new TrackingQueryBus($inner, $this->registry, $this->resolver('user-2'));

        self::assertSame('task-q1', $bus->askAsync(new GetSomethingQuery('p')));
        self::assertSame(['user-2', GetSomethingQuery::class], $this->registry->records['task-q1']);

        // Synchronous asks are not recorded — there is nothing to poll.
        self::assertSame('sync-answer', $bus->ask(new GetSomethingQuery('p')));
        self::assertCount(1, $this->registry->records);
    }
}
