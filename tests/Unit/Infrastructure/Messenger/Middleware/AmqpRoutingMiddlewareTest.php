<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Unit\Infrastructure\Messenger\Middleware;

use Contracts\Demo\Command\DoSomethingCommand;
use Contracts\Demo\Event\SomethingHappened;
use Contracts\Demo\Query\GetSomethingQuery;
use Jwage\PhpAmqpLibMessengerBundle\Transport\AmqpStamp;
use Kraz\MessengerWorkflow\Infrastructure\Messenger\Middleware\AmqpRoutingMiddleware;
use Kraz\MessengerWorkflow\Infrastructure\Messenger\Stamp\MessageIdStamp;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Middleware\StackMiddleware;
use Symfony\Component\Messenger\Stamp\ReceivedStamp;

final class AmqpRoutingMiddlewareTest extends TestCase
{
    private AmqpRoutingMiddleware $middleware;

    protected function setUp(): void
    {
        $this->middleware = new AmqpRoutingMiddleware();
    }

    private function handle(Envelope $envelope): Envelope
    {
        return $this->middleware->handle($envelope, new StackMiddleware());
    }

    public function testCommandGetsDirectExchangeRoutingKeyAndMessageId(): void
    {
        $envelope = $this->handle(new Envelope(new DoSomethingCommand(), [new MessageIdStamp('task-1')]));

        $stamp = $envelope->last(AmqpStamp::class);
        self::assertNotNull($stamp);
        self::assertSame('commands.Demo', $stamp->getRoutingKey());
        self::assertSame('task-1', $stamp->getAttributes()['message_id'] ?? null);
    }

    public function testQueryGetsDirectExchangeRoutingKey(): void
    {
        $envelope = $this->handle(new Envelope(new GetSomethingQuery(), [new MessageIdStamp('task-2')]));

        $stamp = $envelope->last(AmqpStamp::class);
        self::assertNotNull($stamp);
        self::assertSame('queries.Demo', $stamp->getRoutingKey());
    }

    public function testEventGetsTopicExchangeRoutingKey(): void
    {
        $envelope = $this->handle(new Envelope(new SomethingHappened(), [new MessageIdStamp('task-3')]));

        $stamp = $envelope->last(AmqpStamp::class);
        self::assertNotNull($stamp);
        self::assertSame('events.Demo.Event.SomethingHappened', $stamp->getRoutingKey());
    }

    public function testNonWorkflowMessagesAreLeftUntouched(): void
    {
        $envelope = $this->handle(new Envelope(new \stdClass()));

        self::assertNull($envelope->last(AmqpStamp::class));
    }

    public function testReceivedEnvelopesAreNotRestamped(): void
    {
        $envelope = $this->handle(new Envelope(new DoSomethingCommand(), [new ReceivedStamp('app_commands')]));

        self::assertNull($envelope->last(AmqpStamp::class));
    }

    public function testAnExistingAmqpStampIsPreserved(): void
    {
        $existing = new AmqpStamp('custom.key', ['priority' => 5]);

        $envelope = $this->handle(new Envelope(new DoSomethingCommand(), [$existing]));

        self::assertSame($existing, $envelope->last(AmqpStamp::class));
        self::assertCount(1, $envelope->all(AmqpStamp::class));
    }
}
