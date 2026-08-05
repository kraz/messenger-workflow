<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Unit\Infrastructure\Messenger\Middleware;

use Contracts\Demo\Query\GetSomethingQuery;
use Kraz\MessengerWorkflow\Application\Task\Exception\ResultStorageWaitTimeoutException;
use Kraz\MessengerWorkflow\Infrastructure\Messenger\Middleware\QueryResultMiddleware;
use Kraz\MessengerWorkflow\Infrastructure\Messenger\Stamp\MessageIdStamp;
use Kraz\MessengerWorkflow\Infrastructure\Task\InMemoryResultStorage;
use Kraz\MessengerWorkflow\Tests\Fixture\Message\TestEvent;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\UnrecoverableMessageHandlingException;
use Symfony\Component\Messenger\MessageBus;
use Symfony\Component\Messenger\Middleware\MiddlewareInterface;
use Symfony\Component\Messenger\Middleware\StackInterface;
use Symfony\Component\Messenger\Stamp\HandledStamp;
use Symfony\Component\Messenger\Stamp\ReceivedStamp;

final class QueryResultMiddlewareTest extends TestCase
{
    private InMemoryResultStorage $resultStorage;

    protected function setUp(): void
    {
        $this->resultStorage = new InMemoryResultStorage();
    }

    private function bus(bool $withHandler = true): MessageBus
    {
        $middleware = [new QueryResultMiddleware($this->resultStorage)];
        if ($withHandler) {
            $middleware[] = new class implements MiddlewareInterface {
                public function handle(Envelope $envelope, StackInterface $stack): Envelope
                {
                    return $stack->next()->handle($envelope->with(new HandledStamp('the-answer', 'query-handler')), $stack);
                }
            };
        }

        return new MessageBus($middleware);
    }

    public function testAConsumedQueryResultIsWrittenToTheResultStorage(): void
    {
        $this->bus()->dispatch(new Envelope(new GetSomethingQuery('p'), [
            new ReceivedStamp('queries'),
            new MessageIdStamp('task-1'),
        ]));

        self::assertSame('the-answer', $this->resultStorage->await('task-1', 1));
    }

    public function testSenderSideDispatchIsIgnored(): void
    {
        $this->bus()->dispatch(new Envelope(new GetSomethingQuery('p'), [new MessageIdStamp('task-1')]));

        $this->expectException(ResultStorageWaitTimeoutException::class);
        $this->resultStorage->await('task-1', 1);
    }

    public function testNonQueryMessagesAreIgnored(): void
    {
        $this->bus()->dispatch(new Envelope(new TestEvent('p'), [
            new ReceivedStamp('app_events'),
            new MessageIdStamp('task-1'),
        ]));

        $this->expectException(ResultStorageWaitTimeoutException::class);
        $this->resultStorage->await('task-1', 1);
    }

    public function testAMissingMessageIdFailsPermanently(): void
    {
        $this->expectException(UnrecoverableMessageHandlingException::class);
        $this->expectExceptionMessageMatches('/missing its message id/');

        $this->bus()->dispatch(new Envelope(new GetSomethingQuery('p'), [new ReceivedStamp('queries')]));
    }

    public function testNothingIsWrittenWhenNoHandlerRan(): void
    {
        $this->bus(withHandler: false)->dispatch(new Envelope(new GetSomethingQuery('p'), [
            new ReceivedStamp('queries'),
            new MessageIdStamp('task-1'),
        ]));

        $this->expectException(ResultStorageWaitTimeoutException::class);
        $this->resultStorage->await('task-1', 1);
    }
}
