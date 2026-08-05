<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Unit\Infrastructure\Messenger\Middleware;

use Contracts\Demo\Event\SomethingHappened;
use Jwage\PhpAmqpLibMessengerBundle\Transport\AmqpStamp;
use Kraz\MessengerWorkflow\Infrastructure\Messenger\Middleware\OutboxRelayMiddleware;
use Kraz\MessengerWorkflow\Infrastructure\Messenger\Stamp\MessageIdStamp;
use Kraz\MessengerWorkflow\Infrastructure\Messenger\Stamp\ResultTrackedStamp;
use Kraz\MessengerWorkflow\Infrastructure\Messenger\Stamp\StrictOrderStamp;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\LogicException;
use Symfony\Component\Messenger\Middleware\StackMiddleware;
use Symfony\Component\Messenger\Stamp\BusNameStamp;
use Symfony\Component\Messenger\Stamp\HandledStamp;
use Symfony\Component\Messenger\Stamp\ReceivedStamp;
use Symfony\Component\Messenger\Transport\Sender\SenderInterface;
use Symfony\Component\Messenger\Transport\Sender\SendersLocatorInterface;

final class OutboxRelayMiddlewareTest extends TestCase
{
    /**
     * @var \ArrayObject<int, Envelope>
     */
    private \ArrayObject $sentEnvelopes;

    protected function setUp(): void
    {
        $this->sentEnvelopes = new \ArrayObject();
    }

    private function createMiddleware(bool $withSender = true): OutboxRelayMiddleware
    {
        $sender = new class($this->sentEnvelopes) implements SenderInterface {
            /**
             * @param \ArrayObject<int, Envelope> $sent
             */
            public function __construct(private readonly \ArrayObject $sent)
            {
            }

            public function send(Envelope $envelope): Envelope
            {
                $this->sent->append($envelope);

                return $envelope;
            }
        };

        $locator = new class($withSender ? ['events' => $sender] : []) implements SendersLocatorInterface {
            /**
             * @param array<string, SenderInterface> $senders
             */
            public function __construct(private readonly array $senders)
            {
            }

            public function getSenders(Envelope $envelope): iterable
            {
                yield from $this->senders;
            }
        };

        return new OutboxRelayMiddleware($locator);
    }

    public function testRelaysReceivedEnvelopeWithTransferableStampsAndRoutingKey(): void
    {
        $middleware = $this->createMiddleware();

        $envelope = new Envelope(new SomethingHappened('x'), [
            new ReceivedStamp('app_outbox'),
            new MessageIdStamp('id-9'),
            new ResultTrackedStamp(),
            new StrictOrderStamp(),
            new BusNameStamp('relay.bus'),
        ]);

        $result = $middleware->handle($envelope, new StackMiddleware());

        self::assertCount(1, $this->sentEnvelopes);
        $outgoing = $this->sentEnvelopes[0] ?? null;
        self::assertInstanceOf(Envelope::class, $outgoing);

        self::assertInstanceOf(SomethingHappened::class, $outgoing->getMessage());
        self::assertNull($outgoing->last(ReceivedStamp::class), 'The outgoing envelope must be fresh (sendable)');
        self::assertNull($outgoing->last(BusNameStamp::class), 'Non-transferable stamps are dropped');
        self::assertSame('id-9', $outgoing->last(MessageIdStamp::class)?->getMessageId());
        self::assertNotNull($outgoing->last(ResultTrackedStamp::class));
        self::assertNotNull($outgoing->last(StrictOrderStamp::class));

        $amqpStamp = $outgoing->last(AmqpStamp::class);
        self::assertNotNull($amqpStamp);
        self::assertSame('events.Demo.Event.SomethingHappened', $amqpStamp->getRoutingKey());
        self::assertSame('id-9', $amqpStamp->getAttributes()['message_id'] ?? null);

        self::assertNotNull($result->last(HandledStamp::class), 'The consumed envelope is marked handled so the worker acks it');
    }

    public function testDirectDispatchIsRejected(): void
    {
        $middleware = $this->createMiddleware();

        $this->expectException(LogicException::class);
        $this->expectExceptionMessageMatches('/only consumes messages from outbox transports/');

        $middleware->handle(new Envelope(new SomethingHappened()), new StackMiddleware());
    }

    public function testMissingRoutingIsReportedAsMisconfiguration(): void
    {
        $middleware = $this->createMiddleware(withSender: false);

        $this->expectException(LogicException::class);
        $this->expectExceptionMessageMatches('/No broker transport is routed/');

        $middleware->handle(
            new Envelope(new SomethingHappened(), [new ReceivedStamp('app_outbox')]),
            new StackMiddleware(),
        );
    }
}
