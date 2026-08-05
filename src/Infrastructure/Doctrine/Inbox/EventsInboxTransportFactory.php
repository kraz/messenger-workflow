<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Infrastructure\Doctrine\Inbox;

use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;
use Symfony\Component\Messenger\Transport\TransportInterface;

/**
 * Events default to ordered (single-consumer FIFO) delivery and non-transactional
 * handlers; both are overridable per transport.
 */
class EventsInboxTransportFactory extends InboxTransportFactory
{
    protected const string DSN_PREFIX = 'events-inbox://';
    protected const string DEFAULT_TABLE_NAME = 'zz_events_inbox';
    protected const bool DEFAULT_TRANSACTIONAL_HANDLER = false;

    /**
     * @param array<array-key, mixed> $options
     */
    public function createTransport(#[\SensitiveParameter] string $dsn, array $options, SerializerInterface $serializer): TransportInterface
    {
        $options['multiple_consumers'] ??= false;

        return parent::createTransport($dsn, $options, $serializer);
    }
}
