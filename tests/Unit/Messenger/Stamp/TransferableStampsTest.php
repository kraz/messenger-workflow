<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Unit\Messenger\Stamp;

use Kraz\MessengerWorkflow\Messenger\Stamp\EventStamp;
use Kraz\MessengerWorkflow\Messenger\Stamp\MessageIdStamp;
use Kraz\MessengerWorkflow\Messenger\Stamp\ResultStorageStamp;
use Kraz\MessengerWorkflow\Messenger\Stamp\SourceTransportNameStamp;
use Kraz\MessengerWorkflow\Messenger\Stamp\StrictOrderStamp;
use Kraz\MessengerWorkflow\Messenger\Stamp\TargetTransportNameStamp;
use Kraz\MessengerWorkflow\Messenger\Stamp\TransferableStamps;
use Kraz\MessengerWorkflow\Tests\Fixture\Message\TestEvent;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Stamp\DelayStamp;
use Symfony\Component\Messenger\Stamp\ReceivedStamp;

final class TransferableStampsTest extends TestCase
{
    public function testExtractKeepsOnlyTransferableStamps(): void
    {
        $message = new TestEvent('x');
        $envelope = new Envelope($message, [
            new MessageIdStamp('id-1'),
            new StrictOrderStamp(),
            new ResultStorageStamp(),
            new TargetTransportNameStamp('app_events'),
            new SourceTransportNameStamp('app_outbox'),
            // non-transferable:
            new ReceivedStamp('events'),
            new DelayStamp(1000),
            new EventStamp('id-1', 'events.internal.X'),
        ]);

        $extracted = TransferableStamps::extract($envelope);

        self::assertSame($message, $extracted->getMessage());
        self::assertNotNull($extracted->last(MessageIdStamp::class));
        self::assertNotNull($extracted->last(StrictOrderStamp::class));
        self::assertNotNull($extracted->last(ResultStorageStamp::class));
        self::assertSame('app_events', $extracted->last(TargetTransportNameStamp::class)?->getTransportName());
        self::assertSame('app_outbox', $extracted->last(SourceTransportNameStamp::class)?->getTransportName());

        self::assertNull($extracted->last(ReceivedStamp::class));
        self::assertNull($extracted->last(DelayStamp::class));
        self::assertNull($extracted->last(EventStamp::class));
    }

    public function testToMergesTransferableStampsIntoTargetEnvelope(): void
    {
        $source = new Envelope(new TestEvent(), [new MessageIdStamp('id-2'), new DelayStamp(5)]);
        $target = new Envelope(new TestEvent('other'));

        $result = TransferableStamps::to($target, $source);

        self::assertSame('id-2', $result->last(MessageIdStamp::class)?->getMessageId());
        self::assertNull($result->last(DelayStamp::class));
        self::assertSame('other', $result->getMessage()->name);
    }
}
