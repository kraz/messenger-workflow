<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Unit\Infrastructure\Messenger\Middleware;

use Contracts\Demo\Command\DoSomethingCommand;
use Doctrine\DBAL\Connection as DbalConnection;
use Doctrine\DBAL\DriverManager;
use Doctrine\ORM\EntityManagerInterface;
use Doctrine\ORM\UnitOfWork;
use Doctrine\Persistence\ManagerRegistry;
use Doctrine\Persistence\ObjectManager;
use Doctrine\Persistence\ObjectRepository;
use Jwage\PhpAmqpLibMessengerBundle\Transport\AmqpEnvelope;
use Jwage\PhpAmqpLibMessengerBundle\Transport\AmqpReceivedStamp;
use Kraz\MessengerWorkflow\Infrastructure\Doctrine\WorkflowTransportRegistry;
use Kraz\MessengerWorkflow\Infrastructure\Messenger\Middleware\WorkflowTransactionMiddleware;
use PhpAmqpLib\Message\AMQPMessage;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\UnrecoverableMessageHandlingException;
use Symfony\Component\Messenger\MessageBus;
use Symfony\Component\Messenger\Middleware\MiddlewareInterface;
use Symfony\Component\Messenger\Middleware\StackInterface;
use Symfony\Component\Messenger\Stamp\ReceivedStamp;

/**
 * Spec (no-inbox mode): when a broker queue is consumed directly and is mapped via
 * orm_mappings, a plain transaction on the mapped connection wraps the handlers —
 * commit on success, rollback on failure. Unmapped receptions and plain dispatches
 * run without a transaction. (The inbox mode is covered by
 * tests/Integration/Doctrine/InboxTransactionalityTest.php.)
 */
final class WorkflowTransactionMiddlewareTest extends TestCase
{
    private DbalConnection $dbal;
    private DbalConnection $otherDbal;

    /**
     * Entity managers handed to the fake registry, by name.
     *
     * @var array<string, EntityManagerInterface>
     */
    private array $managers = [];

    /**
     * Transaction state observed by each manager when it was flushed.
     *
     * @var array<string, bool>
     */
    private array $flushed = [];

    /**
     * Transaction state observed inside the handler.
     */
    private ?bool $handlerSawTransaction = null;

    protected function setUp(): void
    {
        $this->dbal = DriverManager::getConnection(['driver' => 'pdo_sqlite', 'memory' => true]);
        $this->otherDbal = DriverManager::getConnection(['driver' => 'pdo_sqlite', 'memory' => true]);
        $this->handlerSawTransaction = null;
        $this->managers = [];
        $this->flushed = [];
    }

    private function doctrine(): ManagerRegistry
    {
        return new class($this->dbal, $this->managers) implements ManagerRegistry {
            /**
             * @param array<string, ObjectManager> $managers
             */
            public function __construct(
                private readonly DbalConnection $connection,
                private readonly array $managers,
            ) {
            }

            public function getDefaultConnectionName(): string
            {
                return 'app';
            }

            public function getConnection(?string $name = null): object
            {
                return 'app' === ($name ?? 'app') ? $this->connection : throw new \InvalidArgumentException(\sprintf('Unknown connection "%s".', (string) $name));
            }

            /**
             * @return array<string, object>
             */
            public function getConnections(): array
            {
                return ['app' => $this->connection];
            }

            /**
             * @return array<string, string>
             */
            public function getConnectionNames(): array
            {
                return ['app' => 'doctrine.dbal.app_connection'];
            }

            public function getDefaultManagerName(): string
            {
                return 'app';
            }

            public function getManager(?string $name = null): ObjectManager
            {
                return $this->managers[$name ?? 'app'] ?? throw new \InvalidArgumentException(\sprintf('Unknown manager "%s".', (string) $name));
            }

            /**
             * @return array<string, ObjectManager>
             */
            public function getManagers(): array
            {
                return $this->managers;
            }

            public function resetManager(?string $name = null): ObjectManager
            {
                throw new \InvalidArgumentException('No managers configured.');
            }

            /**
             * @return array<string, string>
             */
            public function getManagerNames(): array
            {
                return array_combine(
                    array_keys($this->managers),
                    array_map(static fn (string $name): string => 'doctrine.orm.'.$name.'_entity_manager', array_keys($this->managers)),
                );
            }

            /**
             * @param class-string $persistentObject
             *
             * @return ObjectRepository<object>
             */
            public function getRepository(string $persistentObject, ?string $persistentManagerName = null): ObjectRepository
            {
                throw new \LogicException('Not supported.');
            }

            public function getManagerForClass(string $class): ?ObjectManager
            {
                return null;
            }
        };
    }

    /**
     * The compiled `connection name → entity manager names` map the compiler pass
     * would produce for the registered managers: the ones on the 'app' connection.
     *
     * @return array<string, list<string>>
     */
    private function compiledConnectionMap(): array
    {
        return ['app' => array_keys(array_filter(
            $this->managers,
            fn (EntityManagerInterface $manager): bool => $manager->getConnection() === $this->dbal,
        ))];
    }

    /**
     * @param array<string, array<string, string>>  $queueOrmBinding
     * @param array<string, list<string>>|null      $connectionEntityManagers null = the map the compiler pass would compile
     */
    private function bus(array $queueOrmBinding, ?\Throwable $handlerFailure = null, bool $flushEntityManagers = true, ?array $connectionEntityManagers = null, bool $debug = false): MessageBus
    {
        $middleware = new WorkflowTransactionMiddleware(
            new WorkflowTransportRegistry(),
            $queueOrmBinding,
            $this->doctrine(),
            $flushEntityManagers,
            $connectionEntityManagers ?? $this->compiledConnectionMap(),
            $debug,
        );
        $handler = new class($this->dbal, $handlerFailure, function (bool $inTransaction): void { $this->handlerSawTransaction = $inTransaction; }) implements MiddlewareInterface {
            public function __construct(
                private readonly DbalConnection $dbal,
                private readonly ?\Throwable $failure,
                private readonly \Closure $observe,
            ) {
            }

            public function handle(Envelope $envelope, StackInterface $stack): Envelope
            {
                ($this->observe)($this->dbal->isTransactionActive());
                if (null !== $this->failure) {
                    throw $this->failure;
                }

                return $stack->next()->handle($envelope, $stack);
            }
        };

        return new MessageBus([$middleware, $handler]);
    }

    private function receivedFromBrokerQueue(string $transport, string $queue): Envelope
    {
        return new Envelope(new DoSomethingCommand('p'), [
            new ReceivedStamp($transport),
            new AmqpReceivedStamp(new AmqpEnvelope(new AMQPMessage('body')), $queue),
        ]);
    }

    public function testAMappedQueueRunsTheHandlerInATransactionAndCommits(): void
    {
        $this->bus(['commands' => ['app_commands' => 'app']])
            ->dispatch($this->receivedFromBrokerQueue('commands', 'app_commands'));

        self::assertTrue($this->handlerSawTransaction, 'The handler ran inside the mapped connection transaction');
        self::assertFalse($this->dbal->isTransactionActive(), 'The transaction was committed');
    }

    public function testAHandlerFailureRollsTheMappedTransactionBack(): void
    {
        $failure = new \RuntimeException('handler blew up');

        try {
            $this->bus(['commands' => ['app_commands' => 'app']], $failure)
                ->dispatch($this->receivedFromBrokerQueue('commands', 'app_commands'));
            self::fail('The handler failure must bubble up');
        } catch (\Symfony\Component\Messenger\Exception\HandlerFailedException $exception) {
            self::assertSame($failure, $exception->getPrevious());
        } catch (\RuntimeException $exception) {
            self::assertSame($failure, $exception);
        }

        self::assertFalse($this->dbal->isTransactionActive(), 'The transaction was rolled back');
    }

    public function testAnUnmappedQueueRunsWithoutATransaction(): void
    {
        $this->bus(['commands' => ['app_commands' => 'app']])
            ->dispatch($this->receivedFromBrokerQueue('commands', 'other_queue'));

        self::assertFalse($this->handlerSawTransaction);
    }

    public function testAMappingToAnUnknownNameFailsPermanently(): void
    {
        $this->expectException(UnrecoverableMessageHandlingException::class);
        $this->expectExceptionMessageMatches('/neither a Doctrine entity manager nor a DBAL connection/');

        $this->bus(['commands' => ['app_commands' => 'nope']])
            ->dispatch($this->receivedFromBrokerQueue('commands', 'app_commands'));
    }

    public function testASenderSideDispatchIsUntouched(): void
    {
        $this->bus(['commands' => ['app_commands' => 'app']])
            ->dispatch(new Envelope(new DoSomethingCommand('p')));

        self::assertFalse($this->handlerSawTransaction);
    }

    /**
     * Registers an entity manager on the given connection, recording whether the
     * transaction was still open when it was flushed.
     */
    private function manager(string $name, DbalConnection $connection, ?\Throwable $flushFailure = null): void
    {
        $manager = self::createStub(EntityManagerInterface::class);
        $manager->method('isOpen')->willReturn(true);
        $manager->method('getConnection')->willReturn($connection);
        $manager->method('flush')->willReturnCallback(function () use ($name, $flushFailure): void {
            $this->flushed[$name] = $this->dbal->isTransactionActive();
            if (null !== $flushFailure) {
                throw $flushFailure;
            }
        });

        $this->managers[$name] = $manager;
    }

    public function testTheTransactionFlushesTheEntityManagersOnItsConnection(): void
    {
        $this->manager('app', $this->dbal);

        $this->bus(['commands' => ['app_commands' => 'app']])
            ->dispatch($this->receivedFromBrokerQueue('commands', 'app_commands'));

        self::assertSame(['app' => true], $this->flushed, 'The manager was flushed INSIDE the transaction');
        self::assertFalse($this->dbal->isTransactionActive(), 'The transaction was committed');
    }

    public function testManagersOnOtherConnectionsAreLeftAlone(): void
    {
        $this->manager('app', $this->dbal);
        $this->manager('other', $this->otherDbal);

        $this->bus(['commands' => ['app_commands' => 'app']])
            ->dispatch($this->receivedFromBrokerQueue('commands', 'app_commands'));

        self::assertSame(['app' => true], $this->flushed, 'Only the transaction\'s own context is flushed');
    }

    public function testTheFlushCanBeTurnedOff(): void
    {
        $this->manager('app', $this->dbal);

        $this->bus(['commands' => ['app_commands' => 'app']], flushEntityManagers: false)
            ->dispatch($this->receivedFromBrokerQueue('commands', 'app_commands'));

        self::assertSame([], $this->flushed, 'flush_entity_managers: false leaves writing to the application');
        self::assertFalse($this->dbal->isTransactionActive(), 'The transaction still commits');
    }

    public function testAFailingFlushRollsTheTransactionBack(): void
    {
        $failure = new \RuntimeException('flush blew up');
        $this->manager('app', $this->dbal, $failure);

        try {
            $this->bus(['commands' => ['app_commands' => 'app']])
                ->dispatch($this->receivedFromBrokerQueue('commands', 'app_commands'));
            self::fail('The flush failure must bubble up');
        } catch (\RuntimeException $exception) {
            self::assertSame($failure, $exception);
        }

        self::assertFalse($this->dbal->isTransactionActive(), 'The transaction was rolled back');
    }

    public function testAnUnmappedQueueFlushesNothing(): void
    {
        $this->manager('app', $this->dbal);

        $this->bus(['commands' => ['app_commands' => 'app']])
            ->dispatch($this->receivedFromBrokerQueue('commands', 'other_queue'));

        self::assertSame([], $this->flushed);
    }

    /**
     * A dispatch carries no ReceivedStamp — which is what stops the outbox, itself
     * published from inside a flush, from re-entering flush() through this middleware.
     */
    public function testASenderSideDispatchFlushesNothing(): void
    {
        $this->manager('app', $this->dbal);

        $this->bus(['commands' => ['app_commands' => 'app']])
            ->dispatch(new Envelope(new DoSomethingCommand('p')));

        self::assertSame([], $this->flushed);
    }

    /**
     * The explicit orm_mappings target is authoritative: it is flushed even when the
     * compiled `connection → managers` map does not know it (e.g. programmatic setups
     * where the compiler pass never ran) — never silently skipped.
     */
    public function testAMappedManagerAbsentFromTheCompiledMapIsStillFlushed(): void
    {
        $this->manager('app', $this->dbal);

        $this->bus(['commands' => ['app_commands' => 'app']], connectionEntityManagers: [])
            ->dispatch($this->receivedFromBrokerQueue('commands', 'app_commands'));

        self::assertSame(['app' => true], $this->flushed, 'The mapped manager was flushed inside the transaction');
    }

    /**
     * Registers an entity manager on the given connection whose unit of work holds a
     * scheduled (persisted but never flushed) entity.
     */
    private function managerWithScheduledInsertions(string $name, DbalConnection $connection): void
    {
        $unitOfWork = self::createStub(UnitOfWork::class);
        $unitOfWork->method('getScheduledEntityInsertions')->willReturn([new \stdClass()]);

        $manager = self::createStub(EntityManagerInterface::class);
        $manager->method('isOpen')->willReturn(true);
        $manager->method('getConnection')->willReturn($connection);
        $manager->method('getUnitOfWork')->willReturn($unitOfWork);

        $this->managers[$name] = $manager;
    }

    public function testDebugModeFailsTheMessageWhenAForeignManagerHoldsScheduledChanges(): void
    {
        $this->manager('app', $this->dbal);
        $this->managerWithScheduledInsertions('other', $this->otherDbal);

        try {
            $this->bus(['commands' => ['app_commands' => 'app']], debug: true)
                ->dispatch($this->receivedFromBrokerQueue('commands', 'app_commands'));
            self::fail('The foreign pending changes must fail the message');
        } catch (\LogicException $exception) {
            self::assertStringContainsString('entity manager "other"', $exception->getMessage());
        }

        self::assertFalse($this->dbal->isTransactionActive(), 'The transaction was rolled back');
    }

    public function testDebugModeLetsCleanForeignManagersPass(): void
    {
        $this->manager('app', $this->dbal);
        $this->manager('other', $this->otherDbal);

        $this->bus(['commands' => ['app_commands' => 'app']], debug: true)
            ->dispatch($this->receivedFromBrokerQueue('commands', 'app_commands'));

        self::assertSame(['app' => true], $this->flushed, 'Only the transaction\'s own context is flushed');
        self::assertFalse($this->dbal->isTransactionActive(), 'The transaction was committed');
    }
}
