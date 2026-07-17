<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Messenger\Doctrine\Transport\Inbox;

use Doctrine\DBAL\Connection as DbalConnection;
use Doctrine\DBAL\Schema\Schema;
use Doctrine\DBAL\Schema\Table;
use Doctrine\ORM\EntityManagerInterface;
use Kraz\MessengerWorkflow\Messenger\Doctrine\Connection;
use Kraz\MessengerWorkflow\Messenger\Doctrine\DbalConnectionComparator;
use Kraz\MessengerWorkflow\Messenger\Doctrine\Transport\Outbox\OutboxTransport;
use Kraz\MessengerWorkflow\Messenger\Transport\TransactionalTransportInterface;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Transport\Receiver\ListableReceiverInterface;
use Symfony\Component\Messenger\Transport\Receiver\MessageCountAwareInterface;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;
use Symfony\Component\Messenger\Transport\SetupableTransportInterface;
use Symfony\Component\Messenger\Transport\TransportInterface;

class InboxTransport implements TransportInterface, SetupableTransportInterface, MessageCountAwareInterface, ListableReceiverInterface, TransactionalTransportInterface
{
    private Connection $connection;
    private SerializerInterface $serializer;
    private InboxReceiver $receiver;
    private InboxSender $sender;

    public function __construct(Connection $connection, SerializerInterface $serializer)
    {
        $this->connection = $connection;
        $this->serializer = $serializer;
    }

    public function get(?int $fetchSize = null): iterable
    {
        return $this->getReceiver()->get($fetchSize);
    }

    public function ack(Envelope $envelope): void
    {
        $this->getReceiver()->ack($envelope);
    }

    public function reject(Envelope $envelope): void
    {
        $this->getReceiver()->reject($envelope);
    }

    public function getMessageCount(): int
    {
        return $this->getReceiver()->getMessageCount();
    }

    public function all(?int $limit = null): iterable
    {
        return $this->getReceiver()->all($limit);
    }

    public function find(mixed $id): ?Envelope
    {
        return $this->getReceiver()->find($id);
    }

    public function send(Envelope $envelope): Envelope
    {
        return $this->getSender()->send($envelope);
    }

    public function setup(): void
    {
        $this->connection->setup();
    }

    public function configureSchema(Schema $schema, DbalConnection $forConnection, \Closure $isSameDatabase): void
    {
        $this->connection->configureSchema($schema, $forConnection, $isSameDatabase);
    }

    /**
     * @return string[]
     */
    public function getExtraSetupSqlForTable(Table $createdTable): array
    {
        return $this->connection->getExtraSetupSqlForTable($createdTable);
    }

    public function getConnection(): Connection
    {
        return $this->connection;
    }

    private function getReceiver(): InboxReceiver
    {
        return $this->receiver ??= new InboxReceiver($this->connection, $this->serializer);
    }

    private function getSender(): InboxSender
    {
        return $this->sender ??= new InboxSender($this->connection, $this->serializer);
    }

    public function beginTransaction(): void
    {
        $this->connection->getDriverConnection()->beginTransaction();
    }

    public function commitTransaction(): void
    {
        $this->connection->getDriverConnection()->commit();
    }

    public function rollbackTransaction(): void
    {
        $this->connection->getDriverConnection()->rollBack();
    }

    public function isTransactionActive(): bool
    {
        return $this->connection->getDriverConnection()->isTransactionActive();
    }

    public function isSameTransactionProvider(mixed $provider): bool
    {
        if ($provider instanceof EntityManagerInterface) {
            $provider = $provider->getConnection();
        }
        if ($provider instanceof self) {
            $provider = $provider->getConnection();
        }
        if ($provider instanceof OutboxTransport) {
            $provider = $provider->getConnection();
        }
        if ($provider instanceof Connection) {
            $provider = $provider->getDriverConnection();
        }
        if (!$provider instanceof DbalConnection) {
            return false;
        }

        return DbalConnectionComparator::isSameDatabase($this->connection->getDriverConnection(), $provider);
    }
}
