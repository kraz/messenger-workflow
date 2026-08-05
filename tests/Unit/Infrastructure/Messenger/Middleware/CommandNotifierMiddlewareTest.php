<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Unit\Infrastructure\Messenger\Middleware;

use Contracts\Demo\Command\DoSomethingCommand;
use Doctrine\DBAL\DriverManager;
use Kraz\MessengerWorkflow\Application\Task\ResultStoragePayload;
use Kraz\MessengerWorkflow\Infrastructure\Doctrine\Connection;
use Kraz\MessengerWorkflow\Infrastructure\Doctrine\Inbox\InboxTransport;
use Kraz\MessengerWorkflow\Infrastructure\Doctrine\Outbox\OutboxTransport;
use Kraz\MessengerWorkflow\Infrastructure\Doctrine\WorkflowTransportRegistry;
use Kraz\MessengerWorkflow\Infrastructure\Messenger\CommandCompletedNotification;
use Kraz\MessengerWorkflow\Infrastructure\Messenger\Middleware\CommandNotifierMiddleware;
use Kraz\MessengerWorkflow\Infrastructure\Messenger\Stamp\MessageIdStamp;
use Kraz\MessengerWorkflow\Infrastructure\Messenger\Stamp\ResultTrackedStamp;
use Kraz\MessengerWorkflow\Infrastructure\Task\InMemoryResultStorage;
use PHPUnit\Framework\TestCase;
use Psr\Container\ContainerInterface;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\UnrecoverableMessageHandlingException;
use Symfony\Component\Messenger\MessageBus;
use Symfony\Component\Messenger\Middleware\MiddlewareInterface;
use Symfony\Component\Messenger\Middleware\StackInterface;
use Symfony\Component\Messenger\Stamp\HandledStamp;
use Symfony\Component\Messenger\Stamp\ReceivedStamp;
use Symfony\Component\Messenger\Transport\Serialization\PhpSerializer;
use Symfony\Component\Uid\Uuid;

final class CommandNotifierMiddlewareTest extends TestCase
{
    private const string RECEIVER = 'app_commands';
    private const string NOTIFIER = 'app_commands_notifier';

    private \Doctrine\DBAL\Connection $dbal;
    private OutboxTransport $notifierTransport;
    private InMemoryResultStorage $resultStorage;
    private WorkflowTransportRegistry $transportRegistry;

    protected function setUp(): void
    {
        $this->dbal = DriverManager::getConnection(['driver' => 'pdo_sqlite', 'memory' => true]);
        $this->notifierTransport = new OutboxTransport(new Connection(['table_name' => 'notifier', 'index_table_name' => 'notifier_idx'], $this->dbal), new PhpSerializer());
        $this->notifierTransport->setup();
        $this->resultStorage = new InMemoryResultStorage();
        $this->transportRegistry = new WorkflowTransportRegistry();
    }

    /**
     * @param array<string, object> $transports
     */
    private function locator(array $transports): ContainerInterface
    {
        return new class($transports) implements ContainerInterface {
            /**
             * @param array<string, object> $transports
             */
            public function __construct(private readonly array $transports)
            {
            }

            public function get(string $id): object
            {
                return $this->transports[$id] ?? throw new \RuntimeException(\sprintf('Transport "%s" is not defined.', $id));
            }

            public function has(string $id): bool
            {
                return isset($this->transports[$id]);
            }
        };
    }

    /**
     * @param array<string, object> $transports
     */
    private function bus(array $transports, mixed $handlerResult = 'the-result', bool $withHandler = true): MessageBus
    {
        $middleware = [
            new CommandNotifierMiddleware($this->locator($transports), $this->transportRegistry, $this->resultStorage),
        ];
        if ($withHandler) {
            $middleware[] = new class($handlerResult) implements MiddlewareInterface {
                public function __construct(private readonly mixed $handlerResult)
                {
                }

                public function handle(Envelope $envelope, StackInterface $stack): Envelope
                {
                    return $stack->next()->handle($envelope->with(new HandledStamp($this->handlerResult, 'test-handler')), $stack);
                }
            };
        }

        return new MessageBus($middleware);
    }

    private function trackedEnvelope(?string $messageId = null): Envelope
    {
        $stamps = [new ReceivedStamp(self::RECEIVER), new ResultTrackedStamp()];
        if (null !== $messageId) {
            $stamps[] = new MessageIdStamp($messageId);
        }

        return new Envelope(new DoSomethingCommand('p'), $stamps);
    }

    public function testTrackedCommandResultIsWrittenToTheNotifierOutbox(): void
    {
        $taskId = (string) Uuid::v7();
        $this->bus([self::NOTIFIER => $this->notifierTransport])->dispatch($this->trackedEnvelope($taskId));

        $rows = [...$this->notifierTransport->all()];
        self::assertCount(1, $rows);
        $notification = $rows[0]->getMessage();
        self::assertInstanceOf(CommandCompletedNotification::class, $notification);
        self::assertSame($taskId, $notification->getCommandId());
        $payload = $notification->restoreResult();
        self::assertInstanceOf(ResultStoragePayload::class, $payload);
        self::assertSame('the-result', $payload->getValue());

        // The notifier worker publishes the result later — never the handler process.
        $this->expectExceptionMessageMatches('/timeout/');
        $this->resultStorage->await($taskId, 1);
    }

    public function testUntrackedCommandsTouchNeitherNotifierNorResultStorage(): void
    {
        $this->bus([self::NOTIFIER => $this->notifierTransport])->dispatch(
            new Envelope(new DoSomethingCommand('p'), [new ReceivedStamp(self::RECEIVER), new MessageIdStamp((string) Uuid::v7())]),
        );

        self::assertSame(0, $this->notifierTransport->getMessageCount());
    }

    public function testSenderSideDispatchIsIgnored(): void
    {
        $this->bus([self::NOTIFIER => $this->notifierTransport])->dispatch(
            new Envelope(new DoSomethingCommand('p'), [new ResultTrackedStamp(), new MessageIdStamp((string) Uuid::v7())]),
        );

        self::assertSame(0, $this->notifierTransport->getMessageCount());
    }

    public function testWithoutANotifierTransportTheResultIsWrittenDirectly(): void
    {
        $taskId = (string) Uuid::v7();
        $this->bus([])->dispatch($this->trackedEnvelope($taskId));

        self::assertSame('the-result', $this->resultStorage->await($taskId, 1));
    }

    public function testAMissingMessageIdFailsPermanently(): void
    {
        $this->expectException(UnrecoverableMessageHandlingException::class);
        $this->expectExceptionMessageMatches('/missing its message id/');

        $this->bus([self::NOTIFIER => $this->notifierTransport])->dispatch($this->trackedEnvelope());
    }

    public function testANonOutboxNotifierTransportFailsPermanently(): void
    {
        $this->expectException(UnrecoverableMessageHandlingException::class);
        $this->expectExceptionMessageMatches('/must be an outbox transport/');

        $this->bus([self::NOTIFIER => new \stdClass()])->dispatch($this->trackedEnvelope((string) Uuid::v7()));
    }

    public function testANotifierOnAnotherDatabaseThanTheTransactionalInboxFailsPermanently(): void
    {
        $otherDbal = DriverManager::getConnection(['driver' => 'pdo_sqlite', 'memory' => true]);
        $inbox = new InboxTransport(new Connection(['table_name' => 'inbox', 'index_table_name' => 'inbox_idx', 'deduplicate' => true], $otherDbal), new PhpSerializer());
        $this->transportRegistry->addInboxTransport(self::RECEIVER, $inbox, true);

        $this->expectException(UnrecoverableMessageHandlingException::class);
        $this->expectExceptionMessageMatches('/same database connection/');

        $this->bus([self::NOTIFIER => $this->notifierTransport])->dispatch($this->trackedEnvelope((string) Uuid::v7()));
    }

    public function testANotifierSharingTheTransactionalInboxConnectionIsAccepted(): void
    {
        $inbox = new InboxTransport(new Connection(['table_name' => 'inbox', 'index_table_name' => 'inbox_idx', 'deduplicate' => true], $this->dbal), new PhpSerializer());
        $this->transportRegistry->addInboxTransport(self::RECEIVER, $inbox, true);

        $this->bus([self::NOTIFIER => $this->notifierTransport])->dispatch($this->trackedEnvelope((string) Uuid::v7()));

        self::assertSame(1, $this->notifierTransport->getMessageCount());
    }
}
