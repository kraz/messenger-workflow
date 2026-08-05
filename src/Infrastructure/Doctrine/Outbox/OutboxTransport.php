<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Infrastructure\Doctrine\Outbox;

use Doctrine\DBAL\Connection as DbalConnection;
use Doctrine\DBAL\Schema\Schema;
use Doctrine\DBAL\Schema\Table;
use Kraz\MessengerWorkflow\Infrastructure\Doctrine\Connection;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Transport\Receiver\KeepaliveReceiverInterface;
use Symfony\Component\Messenger\Transport\Receiver\ListableReceiverInterface;
use Symfony\Component\Messenger\Transport\Receiver\MessageCountAwareInterface;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;
use Symfony\Component\Messenger\Transport\SetupableTransportInterface;
use Symfony\Component\Messenger\Transport\TransportInterface;

class OutboxTransport implements TransportInterface, SetupableTransportInterface, MessageCountAwareInterface, ListableReceiverInterface, KeepaliveReceiverInterface
{
    private OutboxReceiver $receiver;
    private OutboxSender $sender;

    public function __construct(
        private readonly Connection $connection,
        private readonly SerializerInterface $serializer,
    ) {
    }

    public function get(?int $fetchSize = null): iterable
    {
        return $this->getReceiver()->get($fetchSize);
    }

    public function ack(Envelope $envelope): void
    {
        $this->getReceiver()->ack($envelope);
    }

    public function keepalive(Envelope $envelope, ?int $seconds = null): void
    {
        $this->getReceiver()->keepalive($envelope, $seconds);
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
     * @return list<string>
     */
    public function getExtraSetupSqlForTable(Table $createdTable): array
    {
        return $this->connection->getExtraSetupSqlForTable($createdTable);
    }

    public function getConnection(): Connection
    {
        return $this->connection;
    }

    private function getReceiver(): OutboxReceiver
    {
        return $this->receiver ??= new OutboxReceiver($this->connection, $this->serializer);
    }

    private function getSender(): OutboxSender
    {
        return $this->sender ??= new OutboxSender($this->connection, $this->serializer);
    }
}
