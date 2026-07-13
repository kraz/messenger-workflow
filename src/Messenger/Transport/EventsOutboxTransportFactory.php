<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Messenger\Transport;

use Kraz\MessengerWorkflow\Messenger\Doctrine\Transport\Outbox\OutboxTransportFactory;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;
use Symfony\Component\Messenger\Transport\TransportInterface;

class EventsOutboxTransportFactory extends OutboxTransportFactory
{
    public function createTransport(string $dsn, array $options, SerializerInterface $serializer): TransportInterface
    {
        $options['table_name'] = $options['table_name'] ?? 'zz_events_outbox';
        $options['multiple_consumers'] = false;

        return parent::createTransport($dsn, $options, $serializer);
    }

    public function supports(#[\SensitiveParameter] string $dsn, array $options): bool
    {
        return str_starts_with($dsn, 'events-outbox://');
    }
}
