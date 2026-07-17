<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Integration\Doctrine;

use Doctrine\DBAL\Connection as DBALConnection;
use Doctrine\DBAL\DriverManager;
use Kraz\MessengerWorkflow\Messenger\Doctrine\Connection;
use Kraz\MessengerWorkflow\Messenger\Doctrine\Transport\Inbox\InboxTransport;
use Kraz\MessengerWorkflow\Messenger\Doctrine\Transport\Outbox\OutboxTransport;
use Kraz\MessengerWorkflow\Messenger\Stamp\MessageIdStamp;
use Kraz\MessengerWorkflow\Messenger\Stamp\SourceTransportNameStamp;
use Kraz\MessengerWorkflow\Messenger\Stamp\SourceTransportRetryCountStamp;
use Kraz\MessengerWorkflow\Messenger\Stamp\StrictOrderStamp;
use Kraz\MessengerWorkflow\Messenger\Stamp\TargetTransportNameStamp;
use Kraz\MessengerWorkflow\Tests\Fixture\Message\TestCommand;
use Kraz\MessengerWorkflow\Tests\Fixture\Message\TestEvent;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Stamp\ErrorDetailsStamp;
use Symfony\Component\Messenger\Stamp\RedeliveryStamp;
use Symfony\Component\Messenger\Stamp\TransportMessageIdStamp;
use Symfony\Component\Messenger\Stamp\TransportNamesStamp;
use Symfony\Component\Messenger\Transport\Serialization\PhpSerializer;
use Symfony\Component\Uid\Uuid;

/**
 * Spec: Outbox pattern — events stored atomically, relayed FIFO, never lost on failure.
 * Inbox pattern — deduplication by UUID, target transport tracked with the message.
 */
final class OutboxInboxTransportTest extends TestCase
{
    private DBALConnection $dbal;
    private string $suffix;

    protected function setUp(): void
    {
        $this->dbal = DriverManager::getConnection(['driver' => 'pdo_sqlite', 'memory' => true]);
        $this->suffix = bin2hex(random_bytes(4));
    }

    private function outboxTransport(): OutboxTransport
    {
        $connection = new Connection([
            'table_name' => 'outbox_'.$this->suffix,
            'index_table_name' => 'outbox_idx_'.$this->suffix,
            'auto_setup' => true,
        ], $this->dbal);

        return new OutboxTransport($connection, new PhpSerializer());
    }

    private function inboxTransport(): InboxTransport
    {
        $connection = new Connection([
            'table_name' => 'inbox_'.$this->suffix,
            'index_table_name' => 'inbox_idx_'.$this->suffix,
            'deduplicate' => true,
            'auto_setup' => true,
        ], $this->dbal);

        return new InboxTransport($connection, new PhpSerializer());
    }

    public function testOutboxSendRequiresExactlyOneTransportName(): void
    {
        $outbox = $this->outboxTransport();

        try {
            $outbox->send(new Envelope(new TestEvent()));
            self::fail('Expected RuntimeException for missing transport name');
        } catch (\RuntimeException $e) {
            self::assertStringContainsString('missing a transport names', $e->getMessage());
        }

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessageMatches('/more than one transport name/');

        $outbox->send(new Envelope(new TestEvent(), [
            new TransportNamesStamp(['app_outbox', 'other_outbox']),
        ]));
    }

    public function testOutboxRoundTripCarriesSourceTransportAndStrictOrder(): void
    {
        $outbox = $this->outboxTransport();

        $outbox->send(new Envelope(new TestEvent('stored'), [
            new TransportNamesStamp(['app_outbox']),
            new MessageIdStamp('id-out-1'),
        ]));

        $envelopes = iterator_to_array($outbox->get(), false);
        self::assertCount(1, $envelopes);
        $received = $envelopes[0];

        self::assertInstanceOf(TestEvent::class, $received->getMessage());
        self::assertSame('stored', $received->getMessage()->name);
        // publisher worker uses these to route back through event.bus
        self::assertSame('app_outbox', $received->last(SourceTransportNameStamp::class)?->getTransportName());
        self::assertNotNull($received->last(StrictOrderStamp::class));
        self::assertSame(0, $received->last(SourceTransportRetryCountStamp::class)?->getRetryCount());
        self::assertSame('id-out-1', $received->last(MessageIdStamp::class)?->getMessageId());
        // the TransportNamesStamp must not leak into the stored message
        self::assertNull($received->last(TransportNamesStamp::class));
    }

    public function testOutboxRejectKeepsTheMessageAndIncrementsRetryCount(): void
    {
        // Spec: "no lost events even if broker is down" — a failed relay must not delete
        $outbox = $this->outboxTransport();
        $outbox->send(new Envelope(new TestEvent(), [new TransportNamesStamp(['app_outbox'])]));

        [$received] = iterator_to_array($outbox->get(), false);
        $outbox->reject($received->with(ErrorDetailsStamp::create(new \RuntimeException('broker down'))));

        self::assertSame(1, $outbox->getMessageCount());
        [$again] = iterator_to_array($outbox->get(), false);
        self::assertSame(1, $again->last(SourceTransportRetryCountStamp::class)?->getRetryCount());
    }

    public function testOutboxAckRemovesTheMessage(): void
    {
        $outbox = $this->outboxTransport();
        $outbox->send(new Envelope(new TestEvent(), [new TransportNamesStamp(['app_outbox'])]));

        [$received] = iterator_to_array($outbox->get(), false);
        $outbox->ack($received);

        self::assertSame(0, $outbox->getMessageCount());
    }

    public function testInboxStoresTargetTransportName(): void
    {
        $inbox = $this->inboxTransport();

        $inbox->send(new Envelope(new TestCommand(), [
            new TransportNamesStamp(['app_commands']),
            new MessageIdStamp((string) Uuid::v7()),
        ]));

        [$received] = iterator_to_array($inbox->get(), false);
        self::assertSame('app_commands', $received->last(TargetTransportNameStamp::class)?->getTransportName());
        self::assertNotNull($received->last(TransportMessageIdStamp::class));
    }

    public function testInboxDeduplicatesByMessageUuid(): void
    {
        // Spec: "Deduplication by message UUID … exactly-once processing"
        $inbox = $this->inboxTransport();
        $uuid = (string) Uuid::v7();

        $inbox->send(new Envelope(new TestCommand('first'), [
            new TransportNamesStamp(['app_commands']),
            new MessageIdStamp($uuid),
        ]));
        $inbox->send(new Envelope(new TestCommand('duplicate'), [
            new TransportNamesStamp(['app_commands']),
            new MessageIdStamp($uuid),
        ]));

        self::assertSame(1, $inbox->getMessageCount());
    }

    public function testInboxRedeliveryUpdatesTheStoredMessageInPlace(): void
    {
        $inbox = $this->inboxTransport();
        $uuid = (string) Uuid::v7();

        $sent = $inbox->send(new Envelope(new TestCommand('v1'), [
            new TransportNamesStamp(['app_commands']),
            new MessageIdStamp($uuid),
        ]));
        $transportId = $sent->last(TransportMessageIdStamp::class)?->getId();
        self::assertNotNull($transportId);

        $inbox->send(new Envelope(new TestCommand('v2-retried'), [
            new TransportNamesStamp(['app_commands']),
            new MessageIdStamp($uuid),
            new RedeliveryStamp(1),
            new TransportMessageIdStamp($transportId),
        ]));

        self::assertSame(1, $inbox->getMessageCount());
        [$received] = iterator_to_array($inbox->get(), false);
        self::assertSame('v2-retried', $received->getMessage()->payload);
    }

    public function testInboxIsATransactionalTransport(): void
    {
        $inbox = $this->inboxTransport();

        self::assertFalse($inbox->isTransactionActive());
        $inbox->beginTransaction();
        self::assertTrue($inbox->isTransactionActive());
        $inbox->rollbackTransaction();
        self::assertFalse($inbox->isTransactionActive());
    }

    public function testSameTransactionProviderDetection(): void
    {
        // Spec: the command completion notification is written in the same DB transaction
        // as the handler — inbox and notifier outbox must share the transaction provider.
        $inbox = $this->inboxTransport();
        $outboxSameConnection = $this->outboxTransport();

        self::assertTrue($inbox->isSameTransactionProvider($outboxSameConnection));

        $fileDbalA = DriverManager::getConnection(['driver' => 'pdo_sqlite', 'path' => MWF_TEST_VAR_DIR.'/prov_a_'.$this->suffix.'.db']);
        $fileDbalB = DriverManager::getConnection(['driver' => 'pdo_sqlite', 'path' => MWF_TEST_VAR_DIR.'/prov_b_'.$this->suffix.'.db']);
        $inboxOnFileA = new InboxTransport(new Connection(['table_name' => 'i_'.$this->suffix, 'auto_setup' => true], $fileDbalA), new PhpSerializer());
        $outboxOnFileB = new OutboxTransport(new Connection(['table_name' => 'o_'.$this->suffix, 'auto_setup' => true], $fileDbalB), new PhpSerializer());

        self::assertFalse($inboxOnFileA->isSameTransactionProvider($outboxOnFileB));
    }

    /**
     * Regression test: isSameTransactionProvider() used to compare only
     * the intersection of connection params, so two connections whose distinguishing params
     * did not overlap (e.g. sqlite "memory" vs "path") were wrongly considered the same
     * transaction provider.
     */
    public function testDisjointParamConnectionsAreNotTheSameTransactionProvider(): void
    {
        $inbox = $this->inboxTransport(); // in-memory sqlite

        $fileDbal = DriverManager::getConnection(['driver' => 'pdo_sqlite', 'path' => MWF_TEST_VAR_DIR.'/prov_c_'.$this->suffix.'.db']);
        $foreignOutbox = new OutboxTransport(new Connection([
            'table_name' => 'x_'.$this->suffix,
            'auto_setup' => true,
        ], $fileDbal), new PhpSerializer());

        self::assertFalse($inbox->isSameTransactionProvider($foreignOutbox));
    }
}
