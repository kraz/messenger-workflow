<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Unit\Infrastructure\Messenger;

use Kraz\MessengerWorkflow\Infrastructure\Messenger\EventBus;
use Kraz\MessengerWorkflow\Infrastructure\Messenger\OutboxBus;
use Kraz\MessengerWorkflow\Tests\Fixture\Message\TestEvent;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\MessageBusInterface;
use Symfony\Component\Messenger\Stamp\TransportNamesStamp;

final class EventBusTest extends TestCase
{
    /**
     * @var MessageBusInterface&object{envelopes: list<Envelope>}
     */
    private MessageBusInterface $messageBus;

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
    }

    public function testPublishRejectsNonDomainEvents(): void
    {
        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessageMatches('/Invalid event message/');

        new EventBus($this->messageBus)->publish(new \stdClass());
    }

    public function testPublishDispatchesTheEventOnTheEventBus(): void
    {
        $event = new TestEvent('p');
        new EventBus($this->messageBus)->publish($event);

        self::assertCount(1, $this->messageBus->envelopes);
        self::assertSame($event, $this->messageBus->envelopes[0]->getMessage());
    }

    public function testPublishKeepsEnvelopeStamps(): void
    {
        new EventBus($this->messageBus)->publish(new Envelope(new TestEvent('p'), [new TransportNamesStamp(['app_outbox'])]));

        self::assertNotNull($this->messageBus->envelopes[0]->last(TransportNamesStamp::class));
    }

    public function testOutboxBusPublishesThroughItsConfiguredTransport(): void
    {
        $event = new TestEvent('p');
        new OutboxBus($this->messageBus, 'ctx_outbox')->publish($event);

        $envelope = $this->messageBus->envelopes[0];
        self::assertSame($event, $envelope->getMessage());
        self::assertSame(['ctx_outbox'], $envelope->last(TransportNamesStamp::class)?->getTransportNames());
    }
}
