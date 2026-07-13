<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Messenger\Doctrine\Transport\Outbox;

use Doctrine\DBAL\Exception as DBALException;
use Kraz\MessengerWorkflow\Messenger\Doctrine\Connection;
use Kraz\MessengerWorkflow\Messenger\Stamp\SourceTransportNameStamp;
use Kraz\MessengerWorkflow\Messenger\Stamp\StrictOrderStamp;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\TransportException;
use Symfony\Component\Messenger\Stamp\BusNameStamp;
use Symfony\Component\Messenger\Stamp\TransportMessageIdStamp;
use Symfony\Component\Messenger\Stamp\TransportNamesStamp;
use Symfony\Component\Messenger\Transport\Sender\SenderInterface;
use Symfony\Component\Messenger\Transport\Serialization\PhpSerializer;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;

class OutboxSender implements SenderInterface
{
    private Connection $connection;
    private SerializerInterface $serializer;

    public function __construct(Connection $connection, ?SerializerInterface $serializer = null)
    {
        $this->connection = $connection;
        $this->serializer = $serializer ?? new PhpSerializer();
    }

    public function send(Envelope $envelope): Envelope
    {
        $transportNames = $envelope->last(TransportNamesStamp::class)?->getTransportNames() ?? [];
        if (!$transportNames) {
            $msg = $envelope->getMessage();
            throw new \RuntimeException(\sprintf('Can not publish outbox event "%s". The message envelope is missing a transport names!', $msg::class));
        }
        if (\count($transportNames) > 1) {
            $msg = $envelope->getMessage();
            throw new \RuntimeException(\sprintf('Can not publish outbox event "%s". The message envelope has more than one transport name!', $msg::class));
        }

        $encodedEnvelope = $envelope
            ->withoutAll(BusNameStamp::class)
            ->withoutAll(TransportNamesStamp::class)
            ->with(new SourceTransportNameStamp(reset($transportNames)))
            ->with(new StrictOrderStamp());
        $encodedMessage = $this->serializer->encode($encodedEnvelope);

        try {
            $id = $this->connection->send($encodedMessage['body'], $encodedMessage['headers'] ?? []);
        } catch (DBALException $exception) {
            throw new TransportException($exception->getMessage(), 0, $exception);
        }

        return $envelope->with(new TransportMessageIdStamp($id));
    }
}
