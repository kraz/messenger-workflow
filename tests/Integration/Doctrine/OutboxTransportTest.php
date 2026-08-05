<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Integration\Doctrine;

use Contracts\Demo\Event\SomethingHappened;
use Kraz\MessengerWorkflow\Infrastructure\Doctrine\Connection;
use Kraz\MessengerWorkflow\Infrastructure\Doctrine\Outbox\OutboxTransport;
use Kraz\MessengerWorkflow\Infrastructure\Messenger\Stamp\MessageIdStamp;
use Kraz\MessengerWorkflow\Infrastructure\Messenger\Stamp\SourceTransportRetryCountStamp;
use Kraz\MessengerWorkflow\Infrastructure\Messenger\Stamp\StrictOrderStamp;
use Kraz\MessengerWorkflow\Tests\Support\PostgresDbal;
use PHPUnit\Framework\Attributes\Group;
use PHPUnit\Framework\Attributes\RequiresPhpExtension;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Stamp\ErrorDetailsStamp;
use Symfony\Component\Messenger\Stamp\TransportMessageIdStamp;
use Symfony\Component\Messenger\Transport\Serialization\PhpSerializer;

/**
 * Spec: transactional outbox — the INSERT joins the application transaction (atomicity),
 * relaying is FIFO, and a failed relay returns the message to its original state.
 */
#[Group('postgres')]
#[RequiresPhpExtension('pdo_pgsql')]
final class OutboxTransportTest extends TestCase
{
    private \Doctrine\DBAL\Connection $dbal;
    private string $tableName;
    private OutboxTransport $transport;

    protected function setUp(): void
    {
        try {
            $this->dbal = PostgresDbal::createConnection();
            $this->dbal->executeQuery('SELECT 1');
        } catch (\Throwable $e) {
            self::markTestSkipped('PostgreSQL is not reachable: '.$e->getMessage());
        }

        $this->tableName = 'mwf_outbox_'.bin2hex(random_bytes(4));
        $connection = new Connection([
            'table_name' => $this->tableName,
            'index_table_name' => $this->tableName.'_idx',
        ], $this->dbal);
        $this->transport = new OutboxTransport($connection, new PhpSerializer());
        $this->transport->setup();
    }

    protected function tearDown(): void
    {
        if (isset($this->dbal)) {
            $this->dbal->executeStatement(\sprintf('DROP TABLE IF EXISTS "%s"', $this->tableName));
        }
    }

    private function envelope(string $payload): Envelope
    {
        return new Envelope(new SomethingHappened($payload), [new MessageIdStamp('mid-'.$payload)]);
    }

    public function testSendInsideARolledBackTransactionLeavesNoRow(): void
    {
        $this->dbal->beginTransaction();
        $this->transport->send($this->envelope('tx'));
        self::assertSame(1, $this->transport->getMessageCount());
        $this->dbal->rollBack();

        self::assertSame(0, $this->transport->getMessageCount(), 'Rollback must discard the outbox row');
    }

    public function testSendInsideACommittedTransactionKeepsTheRow(): void
    {
        $this->dbal->beginTransaction();
        $this->transport->send($this->envelope('tx'));
        $this->dbal->commit();

        self::assertSame(1, $this->transport->getMessageCount());
    }

    public function testMessagesAreReceivedFifoWithTransferableStamps(): void
    {
        $this->transport->send($this->envelope('a'));
        $this->transport->send($this->envelope('b'));

        $received = [];
        foreach ($this->transport->get(10) as $envelope) {
            $received[] = $envelope;
            $this->transport->ack($envelope);
        }

        self::assertCount(2, $received);
        $first = $received[0]->getMessage();
        self::assertInstanceOf(SomethingHappened::class, $first);
        self::assertSame('a', $first->payload);
        self::assertSame('mid-a', $received[0]->last(MessageIdStamp::class)?->getMessageId());
        self::assertNotNull($received[0]->last(StrictOrderStamp::class), 'The outbox sender marks the flow ordered');
        self::assertNotNull($received[0]->last(TransportMessageIdStamp::class));

        self::assertSame(0, $this->transport->getMessageCount(), 'Acked rows are deleted');
    }

    public function testRejectKeepsTheRowAndIncrementsRetryCount(): void
    {
        $this->transport->send($this->envelope('r'));

        $envelopes = iterator_to_array($this->transport->get(), false);
        self::assertCount(1, $envelopes);

        $failed = $envelopes[0]->with(ErrorDetailsStamp::create(new \RuntimeException('broker down')));
        $this->transport->reject($failed);

        // The message returned to its original state — still available, retry_count bumped.
        self::assertSame(1, $this->transport->getMessageCount());
        $again = iterator_to_array($this->transport->get(), false);
        self::assertCount(1, $again);
        self::assertSame(1, $again[0]->last(SourceTransportRetryCountStamp::class)?->getRetryCount());

        $this->transport->ack($again[0]);
        self::assertSame(0, $this->transport->getMessageCount());
    }
}
