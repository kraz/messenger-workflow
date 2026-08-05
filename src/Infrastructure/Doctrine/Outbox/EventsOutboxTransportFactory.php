<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Infrastructure\Doctrine\Outbox;

use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;
use Symfony\Component\Messenger\Transport\TransportInterface;

class EventsOutboxTransportFactory extends OutboxTransportFactory
{
    /**
     * @param array<array-key, mixed> $options
     */
    public function createTransport(#[\SensitiveParameter] string $dsn, array $options, SerializerInterface $serializer): TransportInterface
    {
        $options['table_name'] ??= 'zz_events_outbox';
        $options['multiple_consumers'] = false;

        return parent::createTransport($dsn, $options, $serializer);
    }

    /**
     * @param array<array-key, mixed> $options
     */
    public function supports(#[\SensitiveParameter] string $dsn, array $options): bool
    {
        return str_starts_with($dsn, 'events-outbox://');
    }
}
