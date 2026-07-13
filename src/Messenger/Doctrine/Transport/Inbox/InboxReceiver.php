<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Messenger\Doctrine\Transport\Inbox;

use Doctrine\DBAL\Exception as DBALException;
use Doctrine\DBAL\Exception\RetryableException;
use Kraz\MessengerWorkflow\Messenger\Doctrine\Connection;
use Kraz\MessengerWorkflow\Messenger\Stamp\MessageIdStamp;
use Kraz\MessengerWorkflow\Messenger\Stamp\TargetTransportNameStamp;
use Kraz\MessengerWorkflow\Messenger\Stamp\WillRetryMessageStamp;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\LogicException;
use Symfony\Component\Messenger\Exception\MessageDecodingFailedException;
use Symfony\Component\Messenger\Exception\TransportException;
use Symfony\Component\Messenger\Stamp\ErrorDetailsStamp;
use Symfony\Component\Messenger\Stamp\RedeliveryStamp;
use Symfony\Component\Messenger\Stamp\TransportMessageIdStamp;
use Symfony\Component\Messenger\Transport\Receiver\ListableReceiverInterface;
use Symfony\Component\Messenger\Transport\Receiver\MessageCountAwareInterface;
use Symfony\Component\Messenger\Transport\Serialization\PhpSerializer;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;

class InboxReceiver implements ListableReceiverInterface, MessageCountAwareInterface
{
    private const MAX_RETRIES = 3;
    private int $retryingSafetyCounter = 0;
    private Connection $connection;
    private SerializerInterface $serializer;

    public function __construct(Connection $connection, ?SerializerInterface $serializer = null)
    {
        $this->connection = $connection;
        $this->serializer = $serializer ?? new PhpSerializer();
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
            $messageId = $envelope->last(MessageIdStamp::class)?->getMessageId();
            $this->connection->ack((string) $this->findTransportMessageIdStamp($envelope)->getId(), $messageId);
        } catch (DBALException $exception) {
            throw new TransportException($exception->getMessage(), 0, $exception);
        }
    }

    public function reject(Envelope $envelope): void
    {
        try {
            $id = (string) $this->findTransportMessageIdStamp($envelope)->getId();
            $messageId = $envelope->last(MessageIdStamp::class)?->getMessageId();
            if ($envelope->all(TargetTransportNameStamp::class)) {
                $retryCount = $envelope->last(RedeliveryStamp::class)?->getRetryCount() ?? 0;
                if ($envelope->all(WillRetryMessageStamp::class)) {
                    $errorDetails = null;
                    if ($errorDetailsStamp = $envelope->last(ErrorDetailsStamp::class)) {
                        $errorDetails = $errorDetailsStamp->getFlattenException()
                            ? $errorDetailsStamp->getFlattenException()->getAsString()
                            : $errorDetailsStamp->getExceptionMessage();
                    }
                    $this->connection->updateRetryCount($id, $retryCount + 1, $errorDetails);
                } else {
                    $this->connection->reject($id, $messageId);
                }
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

    private function findTransportMessageIdStamp(Envelope $envelope): TransportMessageIdStamp
    {
        $doctrineReceivedStamp = $envelope->last(TransportMessageIdStamp::class);

        if (null === $doctrineReceivedStamp) {
            throw new LogicException('No TransportMessageIdStamp found on the Envelope.');
        }

        return $doctrineReceivedStamp;
    }

    private function createEnvelopeFromData(array $data): Envelope
    {
        try {
            $envelope = $this->serializer->decode([
                'body' => $data['body'],
                'headers' => $data['headers'],
            ]);
        } catch (MessageDecodingFailedException $exception) {
            $this->connection->reject($data['id']);

            throw $exception;
        }

        return $envelope
            ->withoutAll(TransportMessageIdStamp::class)
            ->with(
                new TransportMessageIdStamp($data['id']),
            );
    }
}
