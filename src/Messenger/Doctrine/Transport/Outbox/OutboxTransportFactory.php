<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Messenger\Doctrine\Transport\Outbox;

use Doctrine\DBAL\Connection as DbalConnection;
use Doctrine\DBAL\Platforms\PostgreSQLPlatform;
use Doctrine\Persistence\ConnectionRegistry;
use Kraz\MessengerWorkflow\Messenger\Doctrine\Connection;
use Kraz\MessengerWorkflow\Messenger\Doctrine\PostgreSqlConnection;
use Kraz\MessengerWorkflow\Messenger\EventListener\PostgreSqlNotifyOnIdleListener;
use Symfony\Component\Messenger\Exception\TransportException;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;
use Symfony\Component\Messenger\Transport\TransportFactoryInterface;
use Symfony\Component\Messenger\Transport\TransportInterface;

class OutboxTransportFactory implements TransportFactoryInterface
{
    private ConnectionRegistry $registry;
    private ?PostgreSqlNotifyOnIdleListener $notifyOnIdleListener;

    public function __construct(ConnectionRegistry $registry, ?PostgreSqlNotifyOnIdleListener $notifyOnIdleListener = null)
    {
        $this->registry = $registry;
        $this->notifyOnIdleListener = $notifyOnIdleListener;
    }

    public function createTransport(string $dsn, array $options, SerializerInterface $serializer): TransportInterface
    {
        $options['table_name'] = $options['table_name'] ?? 'z_outbox';
        $options['index_table_name'] = $options['index_table_name'] ?? 'z_outbox_index';
        $options['deduplicate'] = $options['deduplicate'] ?? false;
        $useNotify = ($options['use_notify'] ?? true);
        $transportName = $options['transport_name'] ?? null;
        unset($options['transport_name'], $options['use_notify']);
        $configuration = PostgreSqlConnection::buildConfiguration($dsn, $options);

        try {
            /** @var DbalConnection $driverConnection */
            $driverConnection = $this->registry->getConnection($configuration['connection']);
        } catch (\InvalidArgumentException $e) {
            throw new TransportException(\sprintf('Could not find Doctrine connection from Messenger DSN "%s".', $dsn), 0, $e);
        }

        if ($useNotify && $driverConnection->getDatabasePlatform() instanceof PostgreSQLPlatform) {
            $connection = new PostgreSqlConnection($configuration, $driverConnection);

            if (null !== $transportName) {
                $this->notifyOnIdleListener?->addConnection($transportName, $connection);
            }
        } else {
            $connection = new Connection($configuration, $driverConnection);
        }

        return new OutboxTransport($connection, $serializer);
    }

    public function supports(#[\SensitiveParameter] string $dsn, array $options): bool
    {
        return str_starts_with($dsn, 'outbox://');
    }
}
