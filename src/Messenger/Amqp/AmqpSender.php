<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Messenger\Amqp;

use Jwage\PhpAmqpLibMessengerBundle\Transport\AmqpSender as SymfonyAmqpSender;
use Jwage\PhpAmqpLibMessengerBundle\Transport\Connection;
use Kraz\MessengerWorkflow\Messenger\Stamp\AsyncMessageStamp;
use Kraz\MessengerWorkflow\Messenger\Stamp\SourceTransportNameStamp;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Stamp\BusNameStamp;
use Symfony\Component\Messenger\Stamp\DelayStamp;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;

class AmqpSender extends SymfonyAmqpSender
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
    public function send(Envelope $envelope): Envelope
    {
        if ($envelope->all(AsyncMessageStamp::class)) {
            $envelope = $envelope
                ->withoutAll(BusNameStamp::class);
        }
        if ($envelope->all(SourceTransportNameStamp::class)) {
            $envelope = $envelope
                ->withoutAll(BusNameStamp::class)
                ->withoutAll(DelayStamp::class);
        }

        return parent::send($envelope);
    }
}
