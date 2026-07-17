<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Unit\Messenger\Middleware;

use Kraz\MessengerWorkflow\Messenger\Middleware\InboxMiddleware;
use Kraz\MessengerWorkflow\Messenger\Stamp\MessageIdStamp;
use Kraz\MessengerWorkflow\Messenger\Stamp\StrictOrderStamp;
use Kraz\MessengerWorkflow\Messenger\Stamp\TargetTransportNameStamp;
use Kraz\MessengerWorkflow\Tests\Fixture\Message\TestCommand;
use Kraz\MessengerWorkflow\Tests\Support\SpyMessageBus;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\RecoverableMessageHandlingException;
use Symfony\Component\Messenger\Exception\UnrecoverableMessageHandlingException;
use Symfony\Component\Messenger\Middleware\StackMiddleware;
use Symfony\Component\Messenger\Stamp\HandledStamp;
use Symfony\Component\Messenger\Stamp\ReceivedStamp;
use Symfony\Component\Messenger\Stamp\TransportNamesStamp;

final class InboxMiddlewareTest extends TestCase
{
    public function testReceivedMessageIsRelayedToInboxTransportOfTheTargetQueue(): void
    {
        // Spec: "Receiver worker relays broker messages to inbox"
        $inboxBus = new SpyMessageBus();
        $middleware = new InboxMiddleware($inboxBus);

        $envelope = new Envelope(new TestCommand(), [
            new ReceivedStamp('commands'),
            new TargetTransportNameStamp('app_commands'),
            new MessageIdStamp('id-1'),
        ]);

        $result = $middleware->handle($envelope, new StackMiddleware());

        self::assertSame('inbox', $result->last(HandledStamp::class)?->getHandlerName());

        $relayed = $inboxBus->lastEnvelope();
        self::assertSame(['app_commands'], $relayed->last(TransportNamesStamp::class)?->getTransportNames());
        // transferable stamps travel with the message, transport-specific ones do not
        self::assertSame('id-1', $relayed->last(MessageIdStamp::class)?->getMessageId());
        self::assertNull($relayed->last(ReceivedStamp::class));
    }

    public function testMissingTargetTransportNameIsAnError(): void
    {
        $middleware = new InboxMiddleware(new SpyMessageBus());
        $envelope = new Envelope(new TestCommand(), [new ReceivedStamp('commands')]);

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessageMatches('/target transport name can not be determined/');

        $middleware->handle($envelope, new StackMiddleware());
    }

    public function testRelayFailureWithStrictOrderIsUnrecoverable(): void
    {
        $inboxBus = new SpyMessageBus(static function (): never {
            throw new \RuntimeException('db down');
        });
        $middleware = new InboxMiddleware($inboxBus);

        $envelope = new Envelope(new TestCommand(), [
            new ReceivedStamp('commands'),
            new TargetTransportNameStamp('app_commands'),
            new StrictOrderStamp(),
        ]);

        $this->expectException(UnrecoverableMessageHandlingException::class);

        $middleware->handle($envelope, new StackMiddleware());
    }

    public function testRelayFailureWithoutStrictOrderIsRecoverable(): void
    {
        $inboxBus = new SpyMessageBus(static function (): never {
            throw new \RuntimeException('db down');
        });
        $middleware = new InboxMiddleware($inboxBus);

        $envelope = new Envelope(new TestCommand(), [
            new ReceivedStamp('commands'),
            new TargetTransportNameStamp('app_commands'),
        ]);

        $this->expectException(RecoverableMessageHandlingException::class);

        $middleware->handle($envelope, new StackMiddleware());
    }

    public function testOutgoingMessagePassesThrough(): void
    {
        $inboxBus = new SpyMessageBus();
        $middleware = new InboxMiddleware($inboxBus);

        $envelope = new Envelope(new TestCommand());
        $result = $middleware->handle($envelope, new StackMiddleware());

        self::assertNull($result->last(HandledStamp::class));
        self::assertCount(0, $inboxBus->envelopes);
    }
}
