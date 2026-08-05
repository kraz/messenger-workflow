<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Infrastructure\Doctrine\Inbox;

use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;
use Symfony\Component\Messenger\Transport\TransportInterface;

/**
 * Commands change application state: their handlers run in a transaction by default and
 * the inbox supports competing consumers (SKIP LOCKED).
 */
class CommandsInboxTransportFactory extends InboxTransportFactory
{
    protected const string DSN_PREFIX = 'commands-inbox://';
    protected const string DEFAULT_TABLE_NAME = 'zz_commands_inbox';
    protected const bool DEFAULT_TRANSACTIONAL_HANDLER = true;

    /**
     * @param array<array-key, mixed> $options
     */
    public function createTransport(#[\SensitiveParameter] string $dsn, array $options, SerializerInterface $serializer): TransportInterface
    {
        // strict_order requests single-consumer FIFO delivery — it suppresses the
        // competing-consumers default (an explicit multiple_consumers=true then fails
        // in the Connection constructor: the two options are mutually exclusive).
        $dsnQuery = [];
        $rawQuery = parse_url($dsn, \PHP_URL_QUERY);
        parse_str(\is_string($rawQuery) ? $rawQuery : '', $dsnQuery);
        if (!filter_var($options['strict_order'] ?? $dsnQuery['strict_order'] ?? false, \FILTER_VALIDATE_BOOL)) {
            $options['multiple_consumers'] ??= true;
        }

        return parent::createTransport($dsn, $options, $serializer);
    }
}
