<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Integration\Doctrine;

use Doctrine\DBAL\Connection as DBALConnection;
use Kraz\MessengerWorkflow\Messenger\Doctrine\Connection;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Uid\Uuid;

/**
 * Spec: Outbox pattern — FIFO ordering by auto-increment id; Inbox pattern — deduplication
 * by message UUID with a tracked message index.
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

        self::assertTrue($connection->updateRetryCount($id, 3, 'error details text'));

        $row = $connection->find($id);
        self::assertNotNull($row);
        self::assertSame(3, (int) $row['retry_count']);
    }

    public function testUpdateReplacesBodyAndHeaders(): void
    {
        $connection = $this->createConnection();
        $connection->setup();
        $id = $connection->send('body-old', ['a' => '1']);

        self::assertTrue($connection->update($id, 'body-new', ['b' => '2']));

        $row = $connection->find($id);
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
}
