<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Messenger\Transport;

use Kraz\MessengerWorkflow\Messenger\Doctrine\Transport\Inbox\InboxTransportFactory;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;
use Symfony\Component\Messenger\Transport\TransportInterface;

class CommandsInboxTransportFactory extends InboxTransportFactory
{
    public function createTransport(string $dsn, array $options, SerializerInterface $serializer): TransportInterface
    {
        $options['table_name'] = $options['table_name'] ?? 'zz_commands_inbox';
        $options['multiple_consumers'] = $options['multiple_consumers'] ?? true;

        return parent::createTransport($dsn, $options, $serializer);
    }

    public function supports(#[\SensitiveParameter] string $dsn, array $options): bool
    {
        return str_starts_with($dsn, 'commands-inbox://');
    }
}
