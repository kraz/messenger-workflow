<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Infrastructure\Doctrine\Outbox;

use Doctrine\DBAL\Connection as DbalConnection;
use Doctrine\DBAL\Platforms\PostgreSQLPlatform;
use Doctrine\Persistence\ConnectionRegistry;
use Kraz\MessengerWorkflow\Infrastructure\Doctrine\Connection;
use Kraz\MessengerWorkflow\Infrastructure\Doctrine\PostgreSqlConnection;
use Kraz\MessengerWorkflow\Infrastructure\Doctrine\WorkflowTransportRegistry;
use Kraz\MessengerWorkflow\Infrastructure\Messenger\EventListener\PostgreSqlNotifyOnIdleListener;
use Symfony\Component\Messenger\Exception\TransportException;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;
use Symfony\Component\Messenger\Transport\TransportFactoryInterface;
use Symfony\Component\Messenger\Transport\TransportInterface;

/**
 * Creates outbox transports. DSN: outbox://<dbal-connection-name>[?options].
 * The DSN host is the Doctrine DBAL connection name, so the outbox lives in the
 * owning bounded context's database.
 *
 * @implements TransportFactoryInterface<OutboxTransport>
 */
class OutboxTransportFactory implements TransportFactoryInterface
{
    public function __construct(
        private readonly ?ConnectionRegistry $registry = null,
        private readonly ?PostgreSqlNotifyOnIdleListener $notifyOnIdleListener = null,
        private readonly ?WorkflowTransportRegistry $transportRegistry = null,
    ) {
    }

    /**
     * @param array<array-key, mixed> $options
     */
    public function createTransport(#[\SensitiveParameter] string $dsn, array $options, SerializerInterface $serializer): TransportInterface
    {
        if (null === $this->registry) {
            throw new TransportException(\sprintf('The outbox transport "%s" requires DoctrineBundle (the "doctrine" service is not available).', $dsn));
        }

        $options['table_name'] ??= 'z_outbox';
        $options['deduplicate'] ??= false;
        $useNotify = filter_var($options['use_notify'] ?? true, \FILTER_VALIDATE_BOOL);
        $transportName = \is_string($options['transport_name'] ?? null) ? $options['transport_name'] : null;
        $dsnQuery = [];
        $rawQuery = parse_url($dsn, \PHP_URL_QUERY);
        parse_str(\is_string($rawQuery) ? $rawQuery : '', $dsnQuery);
        $hasExplicitIndexTable = isset($options['index_table_name']) || isset($dsnQuery['index_table_name']);
        unset($options['transport_name'], $options['use_notify']);

        $configuration = PostgreSqlConnection::buildConfiguration($dsn, $options);

        // Derive the index table from the RESOLVED table name (the DSN query may
        // override table_name) so distinct transports never share an index table.
        if (!$hasExplicitIndexTable) {
            $tableName = \is_string($configuration['table_name'] ?? null) ? $configuration['table_name'] : 'z_outbox';
            $configuration['index_table_name'] = $tableName.'_index';
        }

        try {
            $driverConnection = $this->registry->getConnection(\is_string($configuration['connection'] ?? null) ? $configuration['connection'] : null);
        } catch (\InvalidArgumentException $e) {
            throw new TransportException(\sprintf('Could not find Doctrine connection from Messenger DSN "%s".', $dsn), 0, $e);
        }
        if (!$driverConnection instanceof DbalConnection) {
            throw new TransportException(\sprintf('Expected a DBAL connection from Messenger DSN "%s", got "%s".', $dsn, get_debug_type($driverConnection)));
        }

        if ($useNotify && $driverConnection->getDatabasePlatform() instanceof PostgreSQLPlatform) {
            $connection = new PostgreSqlConnection($configuration, $driverConnection);

            if (null !== $transportName) {
                $this->notifyOnIdleListener?->addConnection($transportName, $connection);
            }
        } else {
            $connection = new Connection($configuration, $driverConnection);
        }

        $transport = new OutboxTransport($connection, $serializer);

        if (null !== $transportName) {
            $this->transportRegistry?->addOutboxTransport($transportName, $transport);
        }

        return $transport;
    }

    /**
     * @param array<array-key, mixed> $options
     */
    public function supports(#[\SensitiveParameter] string $dsn, array $options): bool
    {
        return str_starts_with($dsn, 'outbox://');
    }
}
