<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Infrastructure\Doctrine\Outbox;

use Doctrine\DBAL\Exception as DBALException;
use Kraz\MessengerWorkflow\Infrastructure\Doctrine\Connection;
use Kraz\MessengerWorkflow\Infrastructure\Messenger\Stamp\StrictOrderStamp;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\TransportException;
use Symfony\Component\Messenger\Stamp\BusNameStamp;
use Symfony\Component\Messenger\Stamp\TransportMessageIdStamp;
use Symfony\Component\Messenger\Stamp\TransportNamesStamp;
use Symfony\Component\Messenger\Transport\Sender\SenderInterface;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;

/**
 * Persists the envelope into the outbox table. The INSERT runs on the bounded
 * context's DBAL connection and therefore automatically joins any active
 * application transaction — that is the transactional-outbox guarantee.
 */
class OutboxSender implements SenderInterface
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
                ->withoutAll(TransportNamesStamp::class)
                ->withoutAll(StrictOrderStamp::class)
                // The outbox is relayed FIFO by a single consumer — the flow is ordered.
                ->with(new StrictOrderStamp()),
        );

        try {
            $id = $this->connection->send($encodedMessage['body'], $encodedMessage['headers'] ?? []);
        } catch (DBALException $exception) {
            throw new TransportException($exception->getMessage(), 0, $exception);
        }

        return $envelope->with(new TransportMessageIdStamp($id ?? ''));
    }
}
