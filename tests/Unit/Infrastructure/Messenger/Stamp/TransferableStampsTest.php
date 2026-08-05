<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Unit\Infrastructure\Messenger\Stamp;

use Kraz\MessengerWorkflow\Infrastructure\Messenger\Stamp\MessageIdStamp;
use Kraz\MessengerWorkflow\Infrastructure\Messenger\Stamp\ResultTrackedStamp;
use Kraz\MessengerWorkflow\Infrastructure\Messenger\Stamp\StrictOrderStamp;
use Kraz\MessengerWorkflow\Infrastructure\Messenger\Stamp\TransferableStamps;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Stamp\BusNameStamp;
use Symfony\Component\Messenger\Stamp\DelayStamp;

final class TransferableStampsTest extends TestCase
{
    public function testExtractKeepsOnlyTransferableStamps(): void
    {
        $message = new \stdClass();
        $source = new Envelope($message, [
            new MessageIdStamp('id-1'),
            new ResultTrackedStamp(),
            new StrictOrderStamp(),
            new BusNameStamp('command.bus'),
            new DelayStamp(1000),
        ]);

        $extracted = TransferableStamps::extract($source);

        self::assertSame($message, $extracted->getMessage());
        self::assertNotNull($extracted->last(MessageIdStamp::class));
        self::assertNotNull($extracted->last(ResultTrackedStamp::class));
        self::assertNotNull($extracted->last(StrictOrderStamp::class));
        self::assertNull($extracted->last(BusNameStamp::class));
        self::assertNull($extracted->last(DelayStamp::class));
    }

    public function testToMergesTransferableStampsIntoTheTargetEnvelope(): void
    {
        $source = new Envelope(new \stdClass(), [new MessageIdStamp('id-2'), new DelayStamp(5)]);
        $target = new Envelope(new \stdClass(), [new BusNameStamp('event.bus')]);

        $merged = TransferableStamps::to($target, $source);

        $stamp = $merged->last(MessageIdStamp::class);
        self::assertNotNull($stamp);
        self::assertSame('id-2', $stamp->getMessageId());
        self::assertNotNull($merged->last(BusNameStamp::class), 'Existing target stamps are kept');
        self::assertNull($merged->last(DelayStamp::class), 'Non-transferable stamps are not copied');
    }

    public function testAllInstancesOfARepeatedTransferableStampAreCopied(): void
    {
        $source = new Envelope(new \stdClass(), [new MessageIdStamp('a'), new MessageIdStamp('b')]);

        $extracted = TransferableStamps::extract($source);

        self::assertCount(2, $extracted->all(MessageIdStamp::class));
    }
}
