<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Messenger\Doctrine\Transport\Inbox;

use Doctrine\DBAL\Exception as DBALException;
use Kraz\MessengerWorkflow\Messenger\Doctrine\Connection;
use Kraz\MessengerWorkflow\Messenger\Stamp\MessageIdStamp;
use Kraz\MessengerWorkflow\Messenger\Stamp\TargetTransportNameStamp;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\TransportException;
use Symfony\Component\Messenger\Stamp\BusNameStamp;
use Symfony\Component\Messenger\Stamp\RedeliveryStamp;
use Symfony\Component\Messenger\Stamp\TransportMessageIdStamp;
use Symfony\Component\Messenger\Stamp\TransportNamesStamp;
use Symfony\Component\Messenger\Transport\Sender\SenderInterface;
use Symfony\Component\Messenger\Transport\Serialization\PhpSerializer;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;

class InboxSender implements SenderInterface
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
            throw new \RuntimeException(\sprintf('Can not publish inbox event "%s". The message envelope is missing a transport names!', $msg::class));
        }
        if (\count($transportNames) > 1) {
            $msg = $envelope->getMessage();
            throw new \RuntimeException(\sprintf('Can not publish inbox event "%s". The message envelope has more than one transport name!', $msg::class));
        }

        $encodedEnvelope = $envelope
            ->withoutAll(BusNameStamp::class)
            ->withoutAll(TargetTransportNameStamp::class)
            ->with(new TargetTransportNameStamp(reset($transportNames)));
        $encodedMessage = $this->serializer->encode($encodedEnvelope);

        try {
            if ($envelope->all(RedeliveryStamp::class)) {
                $id = $envelope->last(TransportMessageIdStamp::class)?->getId();
                if (!$id) {
                    $msg = $envelope->getMessage();
                    throw new \RuntimeException(\sprintf('Can not update inbox event "%s". The message envelope is missing the transport message ID!', $msg::class));
                }
                $this->connection->update($id, $encodedMessage['body'], $encodedMessage['headers'] ?? []);
            } else {
                $messageId = $envelope->last(MessageIdStamp::class)?->getMessageId();
                $id = $this->connection->send($encodedMessage['body'], $encodedMessage['headers'] ?? [], 0, $messageId);
            }
        } catch (DBALException $exception) {
            throw new TransportException($exception->getMessage(), 0, $exception);
        }

        return $envelope->with(new TransportMessageIdStamp($id));
    }
}
