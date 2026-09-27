<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Integration\Doctrine;

use Doctrine\DBAL\Connection as DBALConnection;
use Kraz\MessengerWorkflow\Infrastructure\Doctrine\Connection;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Uid\Uuid;

/**
 * Spec: Outbox pattern — FIFO ordering by auto-increment id; Inbox pattern — deduplication
 * by message UUID with a tracked message index. Ported from the original package.
 */
abstract class AbstractConnectionTestCase extends TestCase
{
    protected string $tableName;
    protected string $indexTableName;

    abstract protected function createDbalConnection(): DBALConnection;

    /**
     * @param array<string, mixed> $options
     */
    abstract protected function createConnection(array $options = []): Connection;

    protected function setUp(): void
    {
        $suffix = bin2hex(random_bytes(4));
        $this->tableName = 'mwf_msg_'.$suffix;
        $this->indexTableName = 'mwf_idx_'.$suffix;
    }

    /**
     * @param array<string, mixed> $options
     *
     * @return array<string, mixed>
     */
    protected function baseOptions(array $options = []): array
    {
        return $options + [
            'table_name' => $this->tableName,
            'index_table_name' => $this->indexTableName,
        ];
    }

    public function testSendAndGetPreservesFifoOrderByAutoIncrementId(): void
    {
        $connection = $this->createConnection();
        $connection->setup();

        $firstId = $connection->send('body-1', ['h' => '1']);
        $secondId = $connection->send('body-2', ['h' => '2']);
        $thirdId = $connection->send('body-3', ['h' => '3']);

        self::assertNotNull($firstId);
        self::assertTrue((int) $secondId > (int) $firstId);
        self::assertTrue((int) $thirdId > (int) $secondId);

        $batch = $connection->get(10);
        self::assertNotNull($batch);
        self::assertSame(['body-1', 'body-2', 'body-3'], array_column($batch, 'body'));
        self::assertSame(['h' => '1'], $batch[0]['headers']);
    }

    public function testFetchSizeLimitsReturnedMessages(): void
    {
        $connection = $this->createConnection();
        $connection->setup();
        $connection->send('body-1', []);
        $connection->send('body-2', []);

        $batch = $connection->get(1);

        self::assertNotNull($batch);
        self::assertCount(1, $batch);
        self::assertSame('body-1', $batch[0]['body']);
    }

    public function testGetReturnsNullWhenQueueIsEmpty(): void
    {
        $connection = $this->createConnection();
        $connection->setup();

        self::assertNull($connection->get());
    }

    public function testAckDeletesTheMessage(): void
    {
        $connection = $this->createConnection();
        $connection->setup();
        $id = $connection->send('body', []);

        self::assertNotNull($id);
        self::assertTrue($connection->ack($id));
        self::assertSame(0, $connection->getMessageCount());
    }

    public function testDeduplicationDropsSecondSendWithSameMessageUuid(): void
    {
        // Spec: "Deduplicates received messages by UUID … exactly-once handler execution"
        $connection = $this->createConnection(['deduplicate' => true]);
        $connection->setup();

        $uuid = (string) Uuid::v7();

        $firstId = $connection->send('body', [], 0, $uuid);
        $duplicateId = $connection->send('body', [], 0, $uuid);

        self::assertNotNull($firstId);
        self::assertNull($duplicateId);
        self::assertSame(1, $connection->getMessageCount());
    }

    public function testDeduplicationRequiresAMessageId(): void
    {
        $connection = $this->createConnection(['deduplicate' => true]);
        $connection->setup();

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessageMatches('/message ID is required/');

        $connection->send('body', []);
    }

    public function testAckWithDeduplicationMarksIndexEntryAsProcessed(): void
    {
        $connection = $this->createConnection(['deduplicate' => true]);
        $connection->setup();
        $uuid = (string) Uuid::v7();
        $id = $connection->send('body', [], 0, $uuid);

        self::assertNotNull($id);
        self::assertTrue($connection->ack($id, $uuid));

        $row = $connection->getDriverConnection()->fetchAssociative(
            "SELECT * FROM {$this->indexTableName} WHERE id = ?",
            [$uuid],
        );
        self::assertNotFalse($row);
        self::assertNotNull($row['processed_at']);
        self::assertSame(0, $connection->getMessageCount());
    }

    public function testUpdateRetryCountAndErrorDetails(): void
    {
        $connection = $this->createConnection();
        $connection->setup();
        $id = $connection->send('body', []);
        self::assertNotNull($id);

        self::assertTrue($connection->updateRetryCount($id, 3, 'error details text'));

        $row = $connection->find($id);
        self::assertNotNull($row);
        self::assertSame(3, $row['retry_count']);
    }

    public function testUpdateReplacesBodyAndHeaders(): void
    {
        $connection = $this->createConnection();
        $connection->setup();
        $id = $connection->send('body-old', ['a' => '1']);
        self::assertNotNull($id);

        self::assertTrue($connection->update($id, 'body-new', ['b' => '2']));

        $row = $connection->find($id);
        self::assertNotNull($row);
        self::assertSame('body-new', $row['body']);
        self::assertSame(['b' => '2'], $row['headers']);
    }

    public function testDelayedMessagesAreNotSupported(): void
    {
        $connection = $this->createConnection();
        $connection->setup();

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('Delay is not supported!');

        $connection->send('body', [], 5000);
    }

    public function testAFutureAvailableAtDefersDeliveryInFifoMode(): void
    {
        $connection = $this->createConnection();
        $connection->setup();
        $id = $connection->send('body', []);
        self::assertNotNull($id);

        $connection->update($id, 'body', [], new \DateTimeImmutable('+1 hour', new \DateTimeZone('UTC')));
        $deferred = $connection->get();
        self::assertNull($deferred, 'A row in retry backoff must not be deliverable');

        $connection->update($id, 'body', [], new \DateTimeImmutable('-1 second', new \DateTimeZone('UTC')));
        $batch = $connection->get();
        self::assertNotNull($batch);
        self::assertSame('body', $batch[0]['body']);
    }

    public function testADelayedFifoHeadBlocksItsSuccessors(): void
    {
        $connection = $this->createConnection();
        $connection->setup();
        $firstId = $connection->send('b1', []);
        $connection->send('b2', []);
        self::assertNotNull($firstId);

        $connection->update($firstId, 'b1', [], new \DateTimeImmutable('+1 hour', new \DateTimeZone('UTC')));
        $blocked = $connection->get(10);
        self::assertNull($blocked, 'FIFO successors must not overtake a head in retry backoff');

        $connection->update($firstId, 'b1', [], null);
        $batch = $connection->get(10);
        self::assertNotNull($batch);
        self::assertSame(['b1', 'b2'], array_column($batch, 'body'), 'Order is preserved once the head becomes available');
    }

    public function testADelayedSuccessorDoesNotBlockTheHead(): void
    {
        $connection = $this->createConnection();
        $connection->setup();
        $connection->send('b1', []);
        $secondId = $connection->send('b2', []);
        self::assertNotNull($secondId);

        $connection->update($secondId, 'b2', [], new \DateTimeImmutable('+1 hour', new \DateTimeZone('UTC')));
        $batch = $connection->get(10);

        self::assertNotNull($batch);
        self::assertSame(['b1'], array_column($batch, 'body'), 'The batch is truncated at the first not-yet-available row');
    }

    public function testFindAllRespectsLimit(): void
    {
        $connection = $this->createConnection();
        $connection->setup();
        $connection->send('b1', []);
        $connection->send('b2', []);
        $connection->send('b3', []);

        self::assertCount(2, $connection->findAll(2));
        self::assertCount(3, $connection->findAll());
    }

    public function testAckStoresProcessedAtInUtc(): void
    {
        $defaultTimezone = date_default_timezone_get();
        date_default_timezone_set('Pacific/Kiritimati'); // UTC+14: a local timestamp would be far off
        try {
            $connection = $this->createConnection(['deduplicate' => true]);
            $connection->setup();
            $uuid = (string) Uuid::v7();
            $id = $connection->send('body', [], 0, $uuid);
            self::assertNotNull($id);
            $connection->ack($id, $uuid);
        } finally {
            date_default_timezone_set($defaultTimezone);
        }

        $processedAt = $connection->getDriverConnection()->fetchOne("SELECT processed_at FROM {$this->indexTableName} WHERE id = ?", [$uuid]);
        self::assertIsString($processedAt);
        $delta = abs(new \DateTimeImmutable($processedAt, new \DateTimeZone('UTC'))->getTimestamp() - time());
        self::assertLessThan(60, $delta, 'processed_at must be written in UTC, like created_at and delivered_at');
    }

    public function testPruneProcessedDeletesOnlyEntriesProcessedBeforeTheCutOff(): void
    {
        $connection = $this->createConnection(['deduplicate' => true]);
        $connection->setup();

        $old = $this->processedEntry($connection, '-10 days');
        $recent = $this->processedEntry($connection, '-1 hour');
        $pending = (string) Uuid::v7();
        self::assertNotNull($connection->send('pending', [], 0, $pending));

        $cutOff = new \DateTimeImmutable('-7 days', new \DateTimeZone('UTC'));
        self::assertSame(1, $connection->countProcessed($cutOff));
        self::assertSame(1, $connection->pruneProcessed($cutOff));

        $expected = [$pending, $recent];
        sort($expected);
        self::assertSame($expected, $this->indexedIds($connection));
        self::assertSame(1, $connection->getMessageCount(), 'The pending inbox row is untouched');
        self::assertNull($connection->send('pending', [], 0, $pending), 'A pending entry still deduplicates');
        self::assertNull($connection->send('recent', [], 0, $recent), 'A retained entry still deduplicates');
        self::assertNotNull($connection->send('old', [], 0, $old), 'A pruned entry no longer deduplicates');
    }

    public function testPruneProcessedWorksInBatches(): void
    {
        $connection = $this->createConnection(['deduplicate' => true]);
        $connection->setup();
        for ($i = 0; $i < 5; ++$i) {
            $this->processedEntry($connection, '-30 days');
        }
        $kept = $this->processedEntry($connection, '-1 day');

        self::assertSame(5, $connection->pruneProcessed(new \DateTimeImmutable('-7 days', new \DateTimeZone('UTC')), 2));
        self::assertSame([$kept], $this->indexedIds($connection));
    }

    public function testPruneProcessedIsANoOpWithoutDeduplicationOrIndexTable(): void
    {
        $cutOff = new \DateTimeImmutable('UTC');

        $plain = $this->createConnection();
        $plain->setup();
        self::assertSame(0, $plain->countProcessed($cutOff));
        self::assertSame(0, $plain->pruneProcessed($cutOff));

        $notSetUp = $this->createConnection(['deduplicate' => true]);
        self::assertSame(0, $notSetUp->countProcessed($cutOff));
        self::assertSame(0, $notSetUp->pruneProcessed($cutOff));
    }

    /**
     * Sends and acks a deduplicated message, then backdates its processed_at.
     */
    private function processedEntry(Connection $connection, string $processedAgo): string
    {
        $uuid = (string) Uuid::v7();
        $id = $connection->send('body', [], 0, $uuid);
        self::assertNotNull($id);
        self::assertTrue($connection->ack($id, $uuid));

        $connection->getDriverConnection()->executeStatement(
            "UPDATE {$this->indexTableName} SET processed_at = ? WHERE id = ?",
            [new \DateTimeImmutable($processedAgo, new \DateTimeZone('UTC')), $uuid],
            [\Doctrine\DBAL\Types\Types::DATETIME_IMMUTABLE],
        );

        return $uuid;
    }

    /**
     * @return list<string>
     */
    private function indexedIds(Connection $connection): array
    {
        $ids = $connection->getDriverConnection()->fetchFirstColumn("SELECT id FROM {$this->indexTableName}");
        $ids = array_map(static fn (mixed $id): string => \is_scalar($id) ? (string) $id : '', $ids);
        sort($ids);

        return $ids;
    }
}
