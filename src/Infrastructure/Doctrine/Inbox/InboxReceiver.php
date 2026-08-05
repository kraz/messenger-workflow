<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Infrastructure\Doctrine\Inbox;

use Doctrine\DBAL\Exception as DBALException;
use Doctrine\DBAL\Exception\RetryableException;
use Kraz\MessengerWorkflow\Infrastructure\Doctrine\Connection;
use Kraz\MessengerWorkflow\Infrastructure\Messenger\Stamp\MessageIdStamp;
use Kraz\MessengerWorkflow\Infrastructure\Messenger\Stamp\WillRetryMessageStamp;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\LogicException;
use Symfony\Component\Messenger\Exception\MessageDecodingFailedException;
use Symfony\Component\Messenger\Exception\TransportException;
use Symfony\Component\Messenger\Stamp\ErrorDetailsStamp;
use Symfony\Component\Messenger\Stamp\RedeliveryStamp;
use Symfony\Component\Messenger\Stamp\TransportMessageIdStamp;
use Symfony\Component\Messenger\Transport\Receiver\KeepaliveReceiverInterface;
use Symfony\Component\Messenger\Transport\Receiver\ListableReceiverInterface;
use Symfony\Component\Messenger\Transport\Receiver\MessageCountAwareInterface;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;

/**
 * Reads inbox rows. ack() deletes the row and marks the dedup index entry processed.
 * reject() of a message that will retry only bumps retry_count (the retry redelivery
 * updated the row in place); a permanent rejection deletes the row AND marks the index
 * entry processed, so the message UUID is handled at most once even after failure —
 * later broker redeliveries of the same UUID are dropped.
 *
 * @phpstan-import-type DoctrineEnvelope from Connection
 */
class InboxReceiver implements ListableReceiverInterface, MessageCountAwareInterface, KeepaliveReceiverInterface
{
    private const int MAX_RETRIES = 3;
    private int $retryingSafetyCounter = 0;

    public function __construct(
        private readonly Connection $connection,
        private readonly SerializerInterface $serializer,
    ) {
    }

    public function get(?int $fetchSize = null): iterable
    {
        try {
            $doctrineEnvelopes = $this->connection->get(max(1, $fetchSize ?? 1));
            $this->retryingSafetyCounter = 0;
        } catch (RetryableException $exception) {
            if (++$this->retryingSafetyCounter >= self::MAX_RETRIES) {
                $this->retryingSafetyCounter = 0;
                throw new TransportException($exception->getMessage(), 0, $exception);
            }

            return [];
        } catch (DBALException $exception) {
            throw new TransportException($exception->getMessage(), 0, $exception);
        }

        if (null === $doctrineEnvelopes) {
            return [];
        }

        return array_map($this->createEnvelopeFromData(...), $doctrineEnvelopes);
    }

    public function ack(Envelope $envelope): void
    {
        try {
            $this->connection->ack(
                $this->findTransportMessageId($envelope),
                $envelope->last(MessageIdStamp::class)?->getMessageId(),
            );
        } catch (DBALException $exception) {
            throw new TransportException($exception->getMessage(), 0, $exception);
        }
    }

    public function keepalive(Envelope $envelope, ?int $seconds = null): void
    {
        try {
            $this->connection->keepalive($this->findTransportMessageId($envelope), $seconds);
        } catch (DBALException $exception) {
            throw new TransportException($exception->getMessage(), 0, $exception);
        }
    }

    public function reject(Envelope $envelope): void
    {
        try {
            $id = $this->findTransportMessageId($envelope);
            $messageId = $envelope->last(MessageIdStamp::class)?->getMessageId();

            if ([] !== $envelope->all(WillRetryMessageStamp::class)) {
                $retryCount = $envelope->last(RedeliveryStamp::class)?->getRetryCount() ?? 0;
                $errorDetails = null;
                $errorDetailsStamp = $envelope->last(ErrorDetailsStamp::class);
                if (null !== $errorDetailsStamp) {
                    $errorDetails = null !== $errorDetailsStamp->getFlattenException()
                        ? $errorDetailsStamp->getFlattenException()->getAsString()
                        : $errorDetailsStamp->getExceptionMessage();
                }
                $this->connection->updateRetryCount($id, $retryCount + 1, $errorDetails);
            } else {
                $this->connection->reject($id, $messageId);
            }
        } catch (DBALException $exception) {
            throw new TransportException($exception->getMessage(), 0, $exception);
        }
    }

    public function getMessageCount(): int
    {
        try {
            return $this->connection->getMessageCount();
        } catch (DBALException $exception) {
            throw new TransportException($exception->getMessage(), 0, $exception);
        }
    }

    public function all(?int $limit = null): iterable
    {
        try {
            $doctrineEnvelopes = $this->connection->findAll($limit);
        } catch (DBALException $exception) {
            throw new TransportException($exception->getMessage(), 0, $exception);
        }

        foreach ($doctrineEnvelopes as $doctrineEnvelope) {
            yield $this->createEnvelopeFromData($doctrineEnvelope);
        }
    }

    public function find(mixed $id): ?Envelope
    {
        try {
            $doctrineEnvelope = $this->connection->find($id);
        } catch (DBALException $exception) {
            throw new TransportException($exception->getMessage(), 0, $exception);
        }

        if (null === $doctrineEnvelope) {
            return null;
        }

        return $this->createEnvelopeFromData($doctrineEnvelope);
    }

    private function findTransportMessageId(Envelope $envelope): string
    {
        $stamp = $envelope->last(TransportMessageIdStamp::class);

        if (null === $stamp) {
            throw new LogicException('No TransportMessageIdStamp found on the Envelope.');
        }

        $id = $stamp->getId();

        return \is_scalar($id) ? (string) $id : throw new LogicException('The TransportMessageIdStamp id must be scalar.');
    }

    /**
     * @param DoctrineEnvelope $data
     */
    private function createEnvelopeFromData(array $data): Envelope
    {
        try {
            $headers = [];
            foreach ($data['headers'] as $name => $value) {
                if (\is_string($value)) {
                    $headers[(string) $name] = $value;
                }
            }

            $envelope = $this->serializer->decode([
                'body' => $data['body'],
                'headers' => $headers,
            ]);
        } catch (MessageDecodingFailedException $exception) {
            $this->connection->reject($data['id']);

            throw $exception;
        }

        return $envelope
            ->withoutAll(TransportMessageIdStamp::class)
            ->with(new TransportMessageIdStamp($data['id']));
    }
}
