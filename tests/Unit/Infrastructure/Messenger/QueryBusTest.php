<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Unit\Infrastructure\Messenger;

use Contracts\Demo\Query\GetSomethingQuery;
use Kraz\MessengerWorkflow\Application\Exception\TaskFailedException;
use Kraz\MessengerWorkflow\Application\Exception\TaskTimeOutException;
use Kraz\MessengerWorkflow\Infrastructure\Messenger\QueryBus;
use Kraz\MessengerWorkflow\Infrastructure\Messenger\Stamp\MessageIdStamp;
use Kraz\MessengerWorkflow\Infrastructure\Messenger\Stamp\ResultTrackedStamp;
use Kraz\MessengerWorkflow\Infrastructure\Task\InMemoryResultStorage;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\MessageBusInterface;
use Symfony\Component\Uid\Uuid;

final class QueryBusTest extends TestCase
{
    /**
     * @var MessageBusInterface&object{envelopes: list<Envelope>}
     */
    private MessageBusInterface $messageBus;
    private InMemoryResultStorage $resultStorage;
    private QueryBus $queryBus;

    protected function setUp(): void
    {
        $this->messageBus = new class implements MessageBusInterface {
            /**
             * @var list<Envelope>
             */
            public array $envelopes = [];

            public function dispatch(object $message, array $stamps = []): Envelope
            {
                return $this->envelopes[] = Envelope::wrap($message, $stamps);
            }
        };
        $this->resultStorage = new InMemoryResultStorage();
        $this->queryBus = new QueryBus($this->messageBus, $this->resultStorage);
    }

    public function testAskAsyncRejectsNonQueryMessages(): void
    {
        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessageMatches('/Invalid query message/');

        $this->queryBus->askAsync(new \stdClass());
    }

    public function testAskAsyncReturnsTheTaskIdAndStampsTheEnvelope(): void
    {
        $taskId = $this->queryBus->askAsync(new GetSomethingQuery('p'));

        self::assertTrue(Uuid::isValid($taskId));
        $envelope = $this->messageBus->envelopes[0];
        self::assertSame($taskId, $envelope->last(MessageIdStamp::class)?->getMessageId());
        self::assertNotNull($envelope->last(ResultTrackedStamp::class), 'Queries always use the result storage');
    }

    public function testAwaitReturnsTheStoredValue(): void
    {
        $this->resultStorage->write('task-1', ['answer' => 42]);

        self::assertSame(['answer' => 42], $this->queryBus->await('task-1'));
    }

    public function testAwaitTimeoutIsTranslatedToTaskTimeOutException(): void
    {
        $this->expectException(TaskTimeOutException::class);

        $this->queryBus->await('01890000-0000-7000-8000-000000000000', 1);
    }

    public function testAwaitPropagatesTaskFailures(): void
    {
        $this->resultStorage->writeError('task-1', 'boom', 7, \RuntimeException::class);

        $this->expectException(TaskFailedException::class);
        $this->queryBus->await('task-1');
    }

    public function testAskIsAskAsyncPlusAwait(): void
    {
        // A pre-stamped id lets the single-process test prefill the result.
        $taskId = (string) Uuid::v7();
        $this->resultStorage->write($taskId, 'the-value');

        $result = $this->queryBus->ask(new Envelope(new GetSomethingQuery('p'), [new MessageIdStamp($taskId)]));

        self::assertSame('the-value', $result);
        self::assertCount(1, $this->messageBus->envelopes, 'ask() dispatched the query before awaiting');
    }
}
