<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Unit\Infrastructure\Messenger\Middleware;

use Contracts\Demo\Command\DoSomethingCommand;
use Contracts\Demo\Query\GetSomethingQuery;
use Kraz\MessengerWorkflow\Infrastructure\Messenger\Middleware\ExactlyOneHandlerMiddleware;
use Kraz\MessengerWorkflow\Tests\Fixture\Message\TestEvent;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\UnrecoverableMessageHandlingException;
use Symfony\Component\Messenger\MessageBus;
use Symfony\Component\Messenger\Middleware\MiddlewareInterface;
use Symfony\Component\Messenger\Middleware\StackInterface;
use Symfony\Component\Messenger\Stamp\HandledStamp;
use Symfony\Component\Messenger\Stamp\ReceivedStamp;

final class ExactlyOneHandlerMiddlewareTest extends TestCase
{
    private function bus(int $handlerCount): MessageBus
    {
        return new MessageBus([
            new ExactlyOneHandlerMiddleware(),
            new class($handlerCount) implements MiddlewareInterface {
                public function __construct(private readonly int $handlerCount)
                {
                }

                public function handle(Envelope $envelope, StackInterface $stack): Envelope
                {
                    for ($i = 0; $i < $this->handlerCount; ++$i) {
                        $envelope = $envelope->with(new HandledStamp('result-'.$i, 'handler-'.$i));
                    }

                    return $stack->next()->handle($envelope, $stack);
                }
            },
        ]);
    }

    public function testASingleHandlerPasses(): void
    {
        $envelope = $this->bus(1)->dispatch(new Envelope(new DoSomethingCommand('p'), [new ReceivedStamp('app_commands')]));

        self::assertCount(1, $envelope->all(HandledStamp::class));
    }

    public function testZeroHandlersOnAConsumedCommandIsAPermanentFailure(): void
    {
        $this->expectException(UnrecoverableMessageHandlingException::class);
        $this->expectExceptionMessageMatches('/handled zero times/');

        $this->bus(0)->dispatch(new Envelope(new DoSomethingCommand('p'), [new ReceivedStamp('app_commands')]));
    }

    public function testMultipleHandlersOnAConsumedCommandIsAPermanentFailure(): void
    {
        $this->expectException(UnrecoverableMessageHandlingException::class);
        $this->expectExceptionMessageMatches('/handled multiple times.*"handler-0", "handler-1"/');

        $this->bus(2)->dispatch(new Envelope(new DoSomethingCommand('p'), [new ReceivedStamp('app_commands')]));
    }

    public function testQueriesAreEnforcedToo(): void
    {
        $this->expectException(UnrecoverableMessageHandlingException::class);

        $this->bus(0)->dispatch(new Envelope(new GetSomethingQuery('p'), [new ReceivedStamp('app_queries')]));
    }

    public function testSenderSideDispatchIsNotEnforced(): void
    {
        // Without a ReceivedStamp no handlers run locally by design: every message
        // goes through the broker.
        $envelope = $this->bus(0)->dispatch(new DoSomethingCommand('p'));

        self::assertCount(0, $envelope->all(HandledStamp::class));
    }

    public function testEventsAreNotSubjectToTheRule(): void
    {
        $envelope = $this->bus(0)->dispatch(new Envelope(new TestEvent('p'), [new ReceivedStamp('app_events')]));

        self::assertCount(0, $envelope->all(HandledStamp::class));
    }
}
