<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Messenger\Amqp;

use Jwage\PhpAmqpLibMessengerBundle\Transport\AmqpReceivedStamp;
use Jwage\PhpAmqpLibMessengerBundle\Transport\AmqpReceiver as SymfonyAmqpReceiver;
use Jwage\PhpAmqpLibMessengerBundle\Transport\Connection;
use Kraz\MessengerWorkflow\Messenger\Stamp\StrictOrderStamp;
use Kraz\MessengerWorkflow\Messenger\Stamp\TargetTransportNameStamp;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;

class AmqpReceiver extends SymfonyAmqpReceiver
{
    private Connection $connection;
    private SerializerInterface $serializer;

    public function __construct(
        Connection $connection,
        SerializerInterface $serializer,
    ) {
        $this->connection = $connection;
        $this->serializer = $serializer;

        parent::__construct($this->connection, $this->serializer);
    }

    #[\Override]
    public function get(?int $fetchSize = null): iterable
    {
        yield from $this->getFromQueues($this->connection->getQueueNames(), $fetchSize);
    }

    #[\Override]
    public function getFromQueues(array $queueNames, ?int $fetchSize = null): iterable
    {
        foreach (parent::getFromQueues($queueNames, $fetchSize) as $envelope) {
            if (!$envelope->all(TargetTransportNameStamp::class)) {
                $queueName = $this->getAMQPReceivedStamp($envelope)->getQueueName();
                $envelope = $envelope->with(new TargetTransportNameStamp($queueName));
            }
            yield $envelope;
        }
    }

    #[\Override]
    public function ack(Envelope $envelope): void
    {
        $amqpEnvelope = $this->getAMQPReceivedStamp($envelope)->getAmqpEnvelope();
        $amqpEnvelope->getAMQPMessage()->ack();
    }

    #[\Override]
    public function reject(Envelope $envelope): void
    {
        $keepMessageOrder = null !== $envelope->last(StrictOrderStamp::class);
        $amqpEnvelope = $this->getAMQPReceivedStamp($envelope)->getAmqpEnvelope();
        $amqpEnvelope->getAMQPMessage()->nack($keepMessageOrder);
    }

    private function getAMQPReceivedStamp(Envelope $envelope): AmqpReceivedStamp
    {
        $amqpReceivedStamp = $envelope->last(AmqpReceivedStamp::class);

        if (null === $amqpReceivedStamp) {
            throw new \LogicException('No "AMQPReceivedStamp" stamp found on the Envelope.');
        }

        return $amqpReceivedStamp;
    }
}
