<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Integration\Doctrine;

use Contracts\Demo\Command\DoSomethingCommand;
use Doctrine\ORM\EntityManagerInterface;
use Doctrine\Persistence\ManagerRegistry;
use Doctrine\Persistence\ObjectManager;
use Doctrine\Persistence\ObjectRepository;
use Kraz\MessengerWorkflow\Infrastructure\Doctrine\Connection;
use Kraz\MessengerWorkflow\Infrastructure\Doctrine\Inbox\InboxTransport;
use Kraz\MessengerWorkflow\Infrastructure\Doctrine\WorkflowTransportRegistry;
use Kraz\MessengerWorkflow\Infrastructure\Messenger\Middleware\WorkflowTransactionMiddleware;
use Kraz\MessengerWorkflow\Infrastructure\Messenger\Stamp\MessageIdStamp;
use Kraz\MessengerWorkflow\Tests\Support\PostgresDbal;
use PHPUnit\Framework\Attributes\Group;
use PHPUnit\Framework\Attributes\RequiresPhpExtension;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\MessageBus;
use Symfony\Component\Messenger\Middleware\MiddlewareInterface;
use Symfony\Component\Messenger\Middleware\StackInterface;
use Symfony\Component\Messenger\Stamp\ReceivedStamp;
use Symfony\Component\Messenger\Transport\Serialization\PhpSerializer;
use Symfony\Component\Uid\Uuid;

/**
 * Spec (hard requirement): when an inbox is used, the handler runs in the SAME
 * database transaction that removes the inbox message. Verified through a second,
 * independent connection observing commit/rollback atomicity.
 */
#[Group('postgres')]
#[RequiresPhpExtension('pdo_pgsql')]
final class InboxTransactionalityTest extends TestCase
{
    private const string TRANSPORT_NAME = 'tx_inbox';

    private \Doctrine\DBAL\Connection $dbal;
    private \Doctrine\DBAL\Connection $observer;
    private string $tableName;
    private string $appTableName;
    private InboxTransport $transport;
    private WorkflowTransactionMiddleware $middleware;

    protected function setUp(): void
    {
        try {
            $this->dbal = PostgresDbal::createConnection();
            $this->dbal->executeQuery('SELECT 1');
        } catch (\Throwable $e) {
            self::markTestSkipped('PostgreSQL is not reachable: '.$e->getMessage());
        }
        $this->observer = PostgresDbal::createConnection();

        $suffix = bin2hex(random_bytes(4));
        $this->tableName = 'mwf_txinbox_'.$suffix;
        $this->appTableName = 'mwf_txapp_'.$suffix;

        $connection = new Connection([
            'table_name' => $this->tableName,
            'index_table_name' => $this->tableName.'_idx',
            'deduplicate' => true,
            'multiple_consumers' => true,
        ], $this->dbal);
        $this->transport = new InboxTransport($connection, new PhpSerializer());
        $this->transport->setup();

        $this->dbal->executeStatement(\sprintf('CREATE TABLE "%s" (payload TEXT NOT NULL)', $this->appTableName));

        $registry = new WorkflowTransportRegistry();
        $registry->addInboxTransport(self::TRANSPORT_NAME, $this->transport, true);
        $this->middleware = new WorkflowTransactionMiddleware($registry);
    }

    protected function tearDown(): void
    {
        if (isset($this->dbal)) {
            if ($this->dbal->isTransactionActive()) {
                $this->dbal->rollBack();
            }
            foreach ([$this->tableName, $this->tableName.'_idx', $this->appTableName] as $table) {
                $this->dbal->executeStatement(\sprintf('DROP TABLE IF EXISTS "%s"', $table));
            }
        }
    }

    private function receiveOne(): Envelope
    {
        $this->transport->send(new Envelope(new DoSomethingCommand('p'), [new MessageIdStamp((string) Uuid::v7())]));

        $envelopes = iterator_to_array($this->transport->get(), false);
        self::assertCount(1, $envelopes);

        return $envelopes[0]->with(new ReceivedStamp(self::TRANSPORT_NAME));
    }

    private function handlerMiddleware(\Closure $handler): MiddlewareInterface
    {
        return new class($handler) implements MiddlewareInterface {
            public function __construct(private readonly \Closure $handler)
            {
            }

            public function handle(Envelope $envelope, StackInterface $stack): Envelope
            {
                ($this->handler)();

                return $stack->next()->handle($envelope, $stack);
            }
        };
    }

    private function observedAppRows(): int
    {
        $count = $this->observer->fetchOne(\sprintf('SELECT COUNT(*) FROM "%s"', $this->appTableName));

        return is_numeric($count) ? (int) $count : -1;
    }

    private function observedInboxRows(): int
    {
        $count = $this->observer->fetchOne(\sprintf('SELECT COUNT(*) FROM "%s"', $this->tableName));

        return is_numeric($count) ? (int) $count : -1;
    }

    public function testHandlerWritesAndInboxRemovalCommitAtomically(): void
    {
        $envelope = $this->receiveOne();

        $bus = new MessageBus([
            $this->middleware,
            $this->handlerMiddleware(function (): void {
                $this->dbal->executeStatement(\sprintf('INSERT INTO "%s" (payload) VALUES (?)', $this->appTableName), ['written']);

                // Mid-transaction: NOTHING is visible to the outside world yet —
                // neither the application write nor the inbox row removal.
                self::assertSame(0, $this->observedAppRows(), 'App write must not be visible before commit');
                self::assertSame(1, $this->observedInboxRows(), 'Inbox row must still exist before commit');
            }),
        ]);

        $bus->dispatch($envelope);

        self::assertSame(1, $this->observedAppRows(), 'App write is committed');
        self::assertSame(0, $this->observedInboxRows(), 'Inbox row removal is committed with it');
        self::assertFalse($this->dbal->isTransactionActive());
    }

    public function testHandlerFailureRollsBackWritesAndKeepsTheInboxRow(): void
    {
        $envelope = $this->receiveOne();

        $bus = new MessageBus([
            $this->middleware,
            $this->handlerMiddleware(function (): void {
                $this->dbal->executeStatement(\sprintf('INSERT INTO "%s" (payload) VALUES (?)', $this->appTableName), ['doomed']);

                throw new \RuntimeException('handler failed');
            }),
        ]);

        try {
            $bus->dispatch($envelope);
            self::fail('The handler failure must bubble up');
        } catch (\Throwable $e) {
            self::assertSame('handler failed', $e->getPrevious()?->getMessage() ?? $e->getMessage());
        }

        self::assertSame(0, $this->observedAppRows(), 'App write is rolled back');
        self::assertSame(1, $this->observedInboxRows(), 'Inbox row survives for retry');
        self::assertFalse($this->dbal->isTransactionActive());
    }

    public function testNonTransactionalTransportIsLeftAlone(): void
    {
        $registry = new WorkflowTransportRegistry();
        $registry->addInboxTransport(self::TRANSPORT_NAME, $this->transport, false);

        $envelope = $this->receiveOne();
        $wasInTransaction = null;

        $bus = new MessageBus([
            new WorkflowTransactionMiddleware($registry),
            $this->handlerMiddleware(function () use (&$wasInTransaction): void {
                $wasInTransaction = $this->dbal->isTransactionActive();
            }),
        ]);

        $bus->dispatch($envelope);

        self::assertFalse($wasInTransaction, 'No transaction for non-transactional inbox transports');
        self::assertSame(1, $this->observedInboxRows(), 'The row is acked by the worker, not the middleware');
    }

    /**
     * A fake registry exposing a single entity manager on the given connection.
     *
     * @param array<string, ObjectManager> $managers
     */
    private function doctrine(array $managers): ManagerRegistry
    {
        return new class($this->dbal, $managers) implements ManagerRegistry {
            /**
             * @param array<string, ObjectManager> $managers
             */
            public function __construct(
                private readonly \Doctrine\DBAL\Connection $connection,
                private readonly array $managers,
            ) {
            }

            public function getDefaultConnectionName(): string
            {
                return 'app';
            }

            public function getConnection(?string $name = null): object
            {
                return $this->connection;
            }

            /** @return array<string, object> */
            public function getConnections(): array
            {
                return ['app' => $this->connection];
            }

            /** @return array<string, string> */
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
                return $this->managers[$name ?? 'app'] ?? throw new \InvalidArgumentException('Unknown manager.');
            }

            /** @return array<string, ObjectManager> */
            public function getManagers(): array
            {
                return $this->managers;
            }

            public function resetManager(?string $name = null): ObjectManager
            {
                throw new \LogicException('Not supported.');
            }

            /** @return array<string, string> */
            public function getManagerNames(): array
            {
                return array_combine(array_keys($this->managers), array_keys($this->managers));
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
     * An entity manager whose flush() performs a real write on the connection —
     * standing in for the ORM writing its pending changes.
     */
    private function flushingManager(\Doctrine\DBAL\Connection $connection, string $payload): EntityManagerInterface
    {
        $manager = self::createStub(EntityManagerInterface::class);
        $manager->method('isOpen')->willReturn(true);
        $manager->method('getConnection')->willReturn($connection);
        $manager->method('flush')->willReturnCallback(function () use ($payload): void {
            $this->dbal->executeStatement(\sprintf('INSERT INTO "%s" (payload) VALUES (?)', $this->appTableName), [$payload]);
        });

        return $manager;
    }

    public function testTheEntityManagerIsFlushedInsideTheInboxTransaction(): void
    {
        $envelope = $this->receiveOne();
        $registry = new WorkflowTransportRegistry();
        $registry->addInboxTransport(self::TRANSPORT_NAME, $this->transport, true);

        $bus = new MessageBus([
            new WorkflowTransactionMiddleware($registry, [], $this->doctrine(['app' => $this->flushingManager($this->dbal, 'flushed')])),
            // The handler writes nothing itself — the unit of work is closed for it.
            $this->handlerMiddleware(function (): void {
                self::assertSame(0, $this->observedAppRows(), 'Nothing is written before the boundary flush');
            }),
        ]);

        $bus->dispatch($envelope);

        self::assertSame(1, $this->observedAppRows(), 'The boundary flush committed with the transaction');
        self::assertSame(0, $this->observedInboxRows(), '…atomically with the inbox row removal');
    }

    public function testAFailingFlushKeepsTheInboxRowForRetry(): void
    {
        $envelope = $this->receiveOne();
        $registry = new WorkflowTransportRegistry();
        $registry->addInboxTransport(self::TRANSPORT_NAME, $this->transport, true);

        $manager = self::createStub(EntityManagerInterface::class);
        $manager->method('isOpen')->willReturn(true);
        $manager->method('getConnection')->willReturn($this->dbal);
        $manager->method('flush')->willThrowException(new \RuntimeException('flush failed'));

        $bus = new MessageBus([
            new WorkflowTransactionMiddleware($registry, [], $this->doctrine(['app' => $manager])),
            $this->handlerMiddleware(function (): void {
                $this->dbal->executeStatement(\sprintf('INSERT INTO "%s" (payload) VALUES (?)', $this->appTableName), ['doomed']);
            }),
        ]);

        try {
            $bus->dispatch($envelope);
            self::fail('The flush failure must bubble up');
        } catch (\Throwable $e) {
            self::assertSame('flush failed', $e->getPrevious()?->getMessage() ?? $e->getMessage());
        }

        self::assertSame(0, $this->observedAppRows(), 'The handler write rolled back with the failed flush');
        self::assertSame(1, $this->observedInboxRows(), 'The message stays for retry — it was never acked');
    }

    public function testTheFlushCanBeTurnedOff(): void
    {
        $envelope = $this->receiveOne();
        $registry = new WorkflowTransportRegistry();
        $registry->addInboxTransport(self::TRANSPORT_NAME, $this->transport, true);

        $bus = new MessageBus([
            new WorkflowTransactionMiddleware($registry, [], $this->doctrine(['app' => $this->flushingManager($this->dbal, 'never')]), false),
            $this->handlerMiddleware(static function (): void {}),
        ]);

        $bus->dispatch($envelope);

        self::assertSame(0, $this->observedAppRows(), 'flush_entity_managers: false writes nothing');
        self::assertSame(0, $this->observedInboxRows(), 'The message is still acked in its transaction');
    }
}
