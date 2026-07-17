<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Unit\Messenger;

use Contracts\Demo\Event\SomethingHappened;
use Kraz\MessengerWorkflow\Messenger\EventBus;
use Kraz\MessengerWorkflow\Messenger\Stamp\EventStamp;
use Kraz\MessengerWorkflow\Messenger\Stamp\StrictOrderStamp;
use Kraz\MessengerWorkflow\Tests\Fixture\Message\TestEvent;
use Kraz\MessengerWorkflow\Tests\Support\SpyMessageBus;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Uid\Uuid;
use Symfony\Component\Uid\UuidV7;

final class EventBusTest extends TestCase
{
    public function testRejectsMessagesNotImplementingDomainEventInterface(): void
    {
        $bus = new EventBus(new SpyMessageBus());

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('Invalid event message');

        $bus->publish(new \stdClass());
    }

    public function testPublishAddsEventStampAndStrictOrderStamp(): void
    {
        $spy = new SpyMessageBus();
        $bus = new EventBus($spy);

        $bus->publish(new TestEvent());

        $envelope = $spy->lastEnvelope();
        $eventStamp = $envelope->last(EventStamp::class);
        self::assertNotNull($eventStamp);
        self::assertInstanceOf(UuidV7::class, Uuid::fromString($eventStamp->getMessageId()));
        self::assertSame(
            'events.internal.Kraz.MessengerWorkflow.Tests.Fixture.Message.TestEvent',
            $eventStamp->getRoutingKey(),
        );
        self::assertNotNull($envelope->last(StrictOrderStamp::class));
    }

    public function testPublicContractEventGetsPublicRoutingKey(): void
    {
        $spy = new SpyMessageBus();
        $bus = new EventBus($spy);

        $bus->publish(new SomethingHappened());

        self::assertSame(
            'events.Demo.Event.SomethingHappened',
            $spy->lastEnvelope()->last(EventStamp::class)?->getRoutingKey(),
        );
    }

    public function testExistingEventStampAndStrictOrderStampAreNotDuplicated(): void
    {
        $spy = new SpyMessageBus();
        $bus = new EventBus($spy);

        $envelope = new Envelope(new TestEvent(), [
            new EventStamp('id-fixed', 'events.custom'),
            new StrictOrderStamp(),
        ]);
        $bus->publish($envelope);

        $result = $spy->lastEnvelope();
        self::assertCount(1, $result->all(EventStamp::class));
        self::assertCount(1, $result->all(StrictOrderStamp::class));
        self::assertSame('id-fixed', $result->last(EventStamp::class)?->getMessageId());
    }
}
