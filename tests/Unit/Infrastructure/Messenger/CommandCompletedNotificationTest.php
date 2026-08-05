<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Unit\Infrastructure\Messenger;

use Kraz\MessengerWorkflow\Application\Task\ResultStoragePayload;
use Kraz\MessengerWorkflow\Infrastructure\Messenger\CommandCompletedNotification;
use PHPUnit\Framework\TestCase;

final class CommandCompletedNotificationTest extends TestCase
{
    public function testScalarResultsRoundTripAsResultStoragePayload(): void
    {
        $notification = CommandCompletedNotification::forResult('task-1', 42);

        $restored = $notification->restoreResult();
        self::assertInstanceOf(ResultStoragePayload::class, $restored);
        self::assertSame(42, $restored->getValue());
        self::assertFalse($restored->isError());
    }

    public function testNullAndArrayResultsRoundTrip(): void
    {
        $null = CommandCompletedNotification::forResult('task-1', null)->restoreResult();
        self::assertInstanceOf(ResultStoragePayload::class, $null);
        self::assertNull($null->getValue());

        $array = CommandCompletedNotification::forResult('task-2', ['a' => 1, 'b' => [2, 3]])->restoreResult();
        self::assertInstanceOf(ResultStoragePayload::class, $array);
        self::assertSame(['a' => 1, 'b' => [2, 3]], $array->getValue());
    }

    public function testObjectResultsAreRestoredAsTheOriginalClass(): void
    {
        $result = new ResultStoragePayload(['answer' => 42]);
        $notification = CommandCompletedNotification::forResult('task-1', $result);

        $restored = $notification->restoreResult();
        self::assertInstanceOf(ResultStoragePayload::class, $restored);
        self::assertSame(['answer' => 42], $restored->getValue());
    }

    public function testThePayloadSurvivesAJsonTransportRoundTrip(): void
    {
        // The notification travels through the notifier outbox with the JSON messenger
        // serializer: the payload array must survive json encode/decode losslessly.
        $original = CommandCompletedNotification::forResult('task-1', ['nested' => ['deep' => 'value'], 'n' => 7]);

        $decoded = json_decode(json_encode($original->getResultPayload(), \JSON_THROW_ON_ERROR), true, 512, \JSON_THROW_ON_ERROR);
        self::assertIsArray($decoded);
        $transported = new CommandCompletedNotification($original->getCommandId(), $decoded);

        $restored = $transported->restoreResult();
        self::assertInstanceOf(ResultStoragePayload::class, $restored);
        self::assertSame(['nested' => ['deep' => 'value'], 'n' => 7], $restored->getValue());
        self::assertSame('task-1', $transported->getCommandId());
    }
}
