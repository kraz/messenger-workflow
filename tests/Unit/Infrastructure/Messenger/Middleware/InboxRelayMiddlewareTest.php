<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Unit\Infrastructure\Messenger\Middleware;

use Contracts\Demo\Command\DoSomethingCommand;
use Jwage\PhpAmqpLibMessengerBundle\Transport\AmqpEnvelope;
use Jwage\PhpAmqpLibMessengerBundle\Transport\AmqpReceivedStamp;
use Kraz\MessengerWorkflow\Infrastructure\Messenger\Middleware\InboxRelayMiddleware;
use Kraz\MessengerWorkflow\Infrastructure\Messenger\Stamp\MessageIdStamp;
use Kraz\MessengerWorkflow\Infrastructure\Messenger\Stamp\ResultTrackedStamp;
use PhpAmqpLib\Message\AMQPMessage;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\LogicException;
use Symfony\Component\Messenger\Middleware\StackMiddleware;
use Symfony\Component\Messenger\Stamp\HandledStamp;
use Symfony\Component\Messenger\Stamp\ReceivedStamp;
use Symfony\Component\Messenger\Stamp\TransportNamesStamp;
use Symfony\Component\Messenger\Transport\Sender\SenderInterface;
use Symfony\Component\Messenger\Transport\Sender\SendersLocatorInterface;

final class InboxRelayMiddlewareTest extends TestCase
{
    /**
     * @var \ArrayObject<int, Envelope>
     */
    private \ArrayObject $sentEnvelopes;

    protected function setUp(): void
    {
        $this->sentEnvelopes = new \ArrayObject();
    }

    private function createMiddleware(bool $withSender = true): InboxRelayMiddleware
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

        $locator = new class($withSender ? ['app_commands' => $sender] : []) implements SendersLocatorInterface {
            /**
             * @param array<string, SenderInterface> $senders
             */
            public function __construct(private readonly array $senders)
            {
            }

            public function getSenders(Envelope $envelope): iterable
            {
                foreach ($envelope->last(TransportNamesStamp::class)?->getTransportNames() ?? [] as $name) {
                    if (\is_string($name) && isset($this->senders[$name])) {
                        yield $name => $this->senders[$name];
                    }
                }
            }
        };

        return new InboxRelayMiddleware($locator);
    }

    private function receivedFromQueue(string $queueName): Envelope
    {
        return new Envelope(new DoSomethingCommand('x'), [
            new ReceivedStamp('commands'),
            new AmqpReceivedStamp(new AmqpEnvelope(new AMQPMessage('body')), $queueName),
            new MessageIdStamp('id-1'),
            new ResultTrackedStamp(),
        ]);
    }

    public function testMovesTheMessageIntoTheInboxTransportNamedAfterTheQueue(): void
    {
        $middleware = $this->createMiddleware();

        $result = $middleware->handle($this->receivedFromQueue('app_commands'), new StackMiddleware());

        self::assertCount(1, $this->sentEnvelopes);
        $outgoing = $this->sentEnvelopes[0] ?? null;
        self::assertInstanceOf(Envelope::class, $outgoing);

        self::assertNull($outgoing->last(ReceivedStamp::class));
        self::assertSame('id-1', $outgoing->last(MessageIdStamp::class)?->getMessageId());
        self::assertNotNull($outgoing->last(ResultTrackedStamp::class));
        self::assertSame(['app_commands'], $outgoing->last(TransportNamesStamp::class)?->getTransportNames());

        self::assertNotNull($result->last(HandledStamp::class));
    }

    public function testDirectDispatchIsRejected(): void
    {
        $middleware = $this->createMiddleware();

        $this->expectException(LogicException::class);
        $this->expectExceptionMessageMatches('/only consumes messages from broker transports/');

        $middleware->handle(new Envelope(new DoSomethingCommand()), new StackMiddleware());
    }

    public function testMissingInboxTransportIsReportedAsMisconfiguration(): void
    {
        $middleware = $this->createMiddleware(withSender: false);

        $this->expectException(LogicException::class);
        $this->expectExceptionMessageMatches('/No inbox transport named "app_commands"/');

        $middleware->handle($this->receivedFromQueue('app_commands'), new StackMiddleware());
    }

    public function testNonAmqpEnvelopeIsRejected(): void
    {
        $middleware = $this->createMiddleware();

        $this->expectException(LogicException::class);
        $this->expectExceptionMessageMatches('/expects messages received from an AMQP queue/');

        $middleware->handle(
            new Envelope(new DoSomethingCommand(), [new ReceivedStamp('commands')]),
            new StackMiddleware(),
        );
    }
}
