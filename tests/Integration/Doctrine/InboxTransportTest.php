<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Integration\Doctrine;

use Contracts\Demo\Command\DoSomethingCommand;
use Kraz\MessengerWorkflow\Infrastructure\Doctrine\Connection;
use Kraz\MessengerWorkflow\Infrastructure\Doctrine\Inbox\InboxTransport;
use Kraz\MessengerWorkflow\Infrastructure\Messenger\Stamp\MessageIdStamp;
use Kraz\MessengerWorkflow\Infrastructure\Messenger\Stamp\WillRetryMessageStamp;
use Kraz\MessengerWorkflow\Tests\Support\PostgresDbal;
use PHPUnit\Framework\Attributes\Group;
use PHPUnit\Framework\Attributes\RequiresPhpExtension;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Stamp\RedeliveryStamp;
use Symfony\Component\Messenger\Stamp\TransportMessageIdStamp;
use Symfony\Component\Messenger\Transport\Serialization\PhpSerializer;
use Symfony\Component\Uid\Uuid;

/**
 * Spec: inbox pattern — deduplication by message UUID; a retrying rejection keeps the
 * row; a permanent rejection removes it AND keeps the UUID marked processed so later
 * redeliveries are dropped (at-most-once handling, even on failure).
 */
#[Group('postgres')]
#[RequiresPhpExtension('pdo_pgsql')]
final class InboxTransportTest extends TestCase
{
    private \Doctrine\DBAL\Connection $dbal;
    private string $tableName;
    private InboxTransport $transport;

    protected function setUp(): void
    {
        try {
            $this->dbal = PostgresDbal::createConnection();
            $this->dbal->executeQuery('SELECT 1');
        } catch (\Throwable $e) {
            self::markTestSkipped('PostgreSQL is not reachable: '.$e->getMessage());
        }

        $this->tableName = 'mwf_inbox_'.bin2hex(random_bytes(4));
        $connection = new Connection([
            'table_name' => $this->tableName,
            'index_table_name' => $this->tableName.'_idx',
            'deduplicate' => true,
        ], $this->dbal);
        $this->transport = new InboxTransport($connection, new PhpSerializer());
        $this->transport->setup();
    }

    protected function tearDown(): void
    {
        if (isset($this->dbal)) {
            foreach ([$this->tableName, $this->tableName.'_idx'] as $table) {
                $this->dbal->executeStatement(\sprintf('DROP TABLE IF EXISTS "%s"', $table));
            }
        }
    }

    private function envelope(string $uuid): Envelope
    {
        return new Envelope(new DoSomethingCommand('p'), [new MessageIdStamp($uuid)]);
    }

    public function testDuplicateDeliveriesAreDroppedByTheDedupIndex(): void
    {
        $uuid = (string) Uuid::v7();

        $first = $this->transport->send($this->envelope($uuid));
        $second = $this->transport->send($this->envelope($uuid));

        self::assertNotNull($first->last(TransportMessageIdStamp::class));
        self::assertNull($second->last(TransportMessageIdStamp::class), 'The duplicate is dropped without a row id');
        self::assertSame(1, $this->transport->getMessageCount());
    }

    public function testRetryRedeliveryUpdatesTheExistingRowInPlace(): void
    {
        $uuid = (string) Uuid::v7();
        $this->transport->send($this->envelope($uuid));

        $envelopes = iterator_to_array($this->transport->get(), false);
        self::assertCount(1, $envelopes);
        $received = $envelopes[0];

        // Symfony's retry listener re-sends the envelope with a RedeliveryStamp.
        $this->transport->send($received->with(new RedeliveryStamp(1)));

        self::assertSame(1, $this->transport->getMessageCount(), 'The redelivery must not create a second row');
    }

    public function testRejectOfARetryingMessageKeepsTheRowAndBumpsRetryCount(): void
    {
        $uuid = (string) Uuid::v7();
        $this->transport->send($this->envelope($uuid));

        $envelopes = iterator_to_array($this->transport->get(), false);
        $received = $envelopes[0]->with(new WillRetryMessageStamp(), new RedeliveryStamp(0));

        $this->transport->reject($received);

        self::assertSame(1, $this->transport->getMessageCount());
        $rowId = $received->last(TransportMessageIdStamp::class)?->getId();
        self::assertNotNull($rowId);
        $row = $this->transport->getConnection()->find($rowId);
        self::assertNotNull($row);
        self::assertSame(1, $row['retry_count']);
    }

    public function testPermanentRejectionEnforcesAtMostOnceHandling(): void
    {
        $uuid = (string) Uuid::v7();
        $this->transport->send($this->envelope($uuid));

        $envelopes = iterator_to_array($this->transport->get(), false);
        $this->transport->reject($envelopes[0]);

        self::assertSame(0, $this->transport->getMessageCount(), 'Permanent rejection removes the row');

        // A later broker redelivery of the same UUID must be dropped: the message was
        // handled (unsuccessfully) at most once and lives in the failure transport.
        $resent = $this->transport->send($this->envelope($uuid));
        self::assertNull($resent->last(TransportMessageIdStamp::class));
        self::assertSame(0, $this->transport->getMessageCount());
    }
}
