<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Infrastructure\Doctrine\Inbox;

use Doctrine\DBAL\Exception as DBALException;
use Kraz\MessengerWorkflow\Infrastructure\Doctrine\Connection;
use Kraz\MessengerWorkflow\Infrastructure\Messenger\Stamp\MessageIdStamp;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\TransportException;
use Symfony\Component\Messenger\Stamp\BusNameStamp;
use Symfony\Component\Messenger\Stamp\RedeliveryStamp;
use Symfony\Component\Messenger\Stamp\TransportMessageIdStamp;
use Symfony\Component\Messenger\Stamp\TransportNamesStamp;
use Symfony\Component\Messenger\Transport\Sender\SenderInterface;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;

/**
 * Persists received broker messages into the inbox table, deduplicating by the
 * message UUID (MessageIdStamp): a redelivered UUID that was already stored or
 * processed is dropped. A Symfony retry redelivery UPDATEs the existing row in
 * place instead of inserting (the dedup index would otherwise drop it).
 */
class InboxSender implements SenderInterface
{
    public function __construct(
        private readonly Connection $connection,
        private readonly SerializerInterface $serializer,
    ) {
    }

    public function send(Envelope $envelope): Envelope
    {
        $encodedMessage = $this->serializer->encode(
            $envelope
                ->withoutAll(BusNameStamp::class)
                ->withoutAll(TransportNamesStamp::class),
        );

        try {
            if ([] !== $envelope->all(RedeliveryStamp::class)) {
                $id = $envelope->last(TransportMessageIdStamp::class)?->getId();
                if (!\is_int($id) && !\is_string($id)) {
                    throw new \RuntimeException(\sprintf('Can not update inbox message "%s". The message envelope is missing the transport message ID!', get_debug_type($envelope->getMessage())));
                }
                $this->connection->update($id, $encodedMessage['body'], $encodedMessage['headers'] ?? []);
            } else {
                $messageId = $envelope->last(MessageIdStamp::class)?->getMessageId();
                $id = $this->connection->send($encodedMessage['body'], $encodedMessage['headers'] ?? [], 0, $messageId);
                if (null === $id) {
                    // Duplicate delivery of an already stored/processed message — dropped.
                    return $envelope;
                }
            }
        } catch (DBALException $exception) {
            throw new TransportException($exception->getMessage(), 0, $exception);
        }

        return $envelope->with(new TransportMessageIdStamp($id));
    }
}
