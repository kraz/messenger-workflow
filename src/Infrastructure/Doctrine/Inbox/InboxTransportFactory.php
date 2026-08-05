<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Infrastructure\Doctrine\Inbox;

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
 * Creates inbox transports. DSN: inbox://<dbal-connection-name>[?options].
 * The DSN host is the Doctrine DBAL connection name, so the inbox lives in the
 * handling bounded context's database — which lets the handler transaction span
 * the inbox row removal and the application state changes.
 *
 * Extra DSN options (beyond the Connection options):
 * * transactional_handler: run handlers inside a DB transaction that also removes
 *   the inbox row (commands default: true, events default: false)
 *
 * @implements TransportFactoryInterface<InboxTransport>
 */
class InboxTransportFactory implements TransportFactoryInterface
{
    protected const string DSN_PREFIX = 'inbox://';
    protected const string DEFAULT_TABLE_NAME = 'z_inbox';
    protected const bool DEFAULT_TRANSACTIONAL_HANDLER = false;

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
            throw new TransportException(\sprintf('The inbox transport "%s" requires DoctrineBundle (the "doctrine" service is not available).', $dsn));
        }

        $options['table_name'] ??= static::DEFAULT_TABLE_NAME;
        $options['deduplicate'] ??= true;
        $useNotify = filter_var($options['use_notify'] ?? true, \FILTER_VALIDATE_BOOL);
        $transportName = \is_string($options['transport_name'] ?? null) ? $options['transport_name'] : null;

        // The DSN query may also carry options — extract it before validation.
        $dsnQuery = [];
        $rawQuery = parse_url($dsn, \PHP_URL_QUERY);
        parse_str(\is_string($rawQuery) ? $rawQuery : '', $dsnQuery);
        $transactionalHandler = filter_var(
            $options['transactional_handler'] ?? $dsnQuery['transactional_handler'] ?? static::DEFAULT_TRANSACTIONAL_HANDLER,
            \FILTER_VALIDATE_BOOL,
        );
        $dsn = (string) preg_replace('/([?&])transactional_handler=[^&]*(&|$)/', '$1', $dsn);
        $dsn = rtrim($dsn, '?&');
        $hasExplicitIndexTable = isset($options['index_table_name']) || isset($dsnQuery['index_table_name']);
        unset($options['transport_name'], $options['use_notify'], $options['transactional_handler']);

        $configuration = PostgreSqlConnection::buildConfiguration($dsn, $options);

        // Derive the dedup index table from the RESOLVED table name (the DSN query may
        // override table_name) — a shared default index table across inboxes on the
        // same connection would deduplicate messages across transports.
        if (!$hasExplicitIndexTable) {
            $tableName = \is_string($configuration['table_name'] ?? null) ? $configuration['table_name'] : static::DEFAULT_TABLE_NAME;
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

        $transport = new InboxTransport($connection, $serializer);

        if (null !== $transportName) {
            $this->transportRegistry?->addInboxTransport($transportName, $transport, $transactionalHandler);
        }

        return $transport;
    }

    /**
     * @param array<array-key, mixed> $options
     */
    public function supports(#[\SensitiveParameter] string $dsn, array $options): bool
    {
        return str_starts_with($dsn, static::DSN_PREFIX);
    }
}
