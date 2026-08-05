<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Unit\Infrastructure\Messenger\Middleware;

use Contracts\Demo\Command\DoSomethingCommand;
use Doctrine\DBAL\Connection as DbalConnection;
use Doctrine\DBAL\DriverManager;
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

    /**
     * Transaction state observed inside the handler.
     */
    private ?bool $handlerSawTransaction = null;

    protected function setUp(): void
    {
        $this->dbal = DriverManager::getConnection(['driver' => 'pdo_sqlite', 'memory' => true]);
        $this->handlerSawTransaction = null;
    }

    private function doctrine(): ManagerRegistry
    {
        return new class($this->dbal) implements ManagerRegistry {
            public function __construct(private readonly DbalConnection $connection)
            {
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
                throw new \InvalidArgumentException('No managers configured.');
            }

            /**
             * @return array<string, ObjectManager>
             */
            public function getManagers(): array
            {
                return [];
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
                return [];
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
     * @param array<string, array<string, string>> $queueOrmBinding
     */
    private function bus(array $queueOrmBinding, ?\Throwable $handlerFailure = null): MessageBus
    {
        $middleware = new WorkflowTransactionMiddleware(new WorkflowTransportRegistry(), $queueOrmBinding, $this->doctrine());
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
}
