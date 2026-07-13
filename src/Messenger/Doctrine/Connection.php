<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Messenger\Doctrine;

use Doctrine\DBAL\ArrayParameterType;
use Doctrine\DBAL\Connection as DBALConnection;
use Doctrine\DBAL\Exception as DBALException;
use Doctrine\DBAL\Exception\TableNotFoundException;
use Doctrine\DBAL\Platforms\PostgreSQLPlatform;
use Doctrine\DBAL\Query\ForUpdate\ConflictResolutionMode;
use Doctrine\DBAL\Query\QueryBuilder;
use Doctrine\DBAL\Result as DBALResult;
use Doctrine\DBAL\Schema\AbstractAsset;
use Doctrine\DBAL\Schema\Name\Identifier;
use Doctrine\DBAL\Schema\Name\UnqualifiedName;
use Doctrine\DBAL\Schema\NamedObject;
use Doctrine\DBAL\Schema\PrimaryKeyConstraint;
use Doctrine\DBAL\Schema\Schema;
use Doctrine\DBAL\Schema\Table;
use Doctrine\DBAL\Types\Types;
use Symfony\Component\Messenger\Exception\InvalidArgumentException;
use Symfony\Component\Messenger\Exception\TransportException;
use Symfony\Contracts\Service\ResetInterface;

class Connection implements ResetInterface
{
    protected const string TABLE_OPTION_NAME = '_symfony_messenger_table_name';

    protected const array DEFAULT_OPTIONS = [
        'table_name' => 'messenger_messages',
        'index_table_name' => 'messenger_messages_index',
        'redeliver_timeout' => 5 * 60,
        'multiple_consumers' => false,
        'deduplicate' => false,
        'auto_setup' => false,
    ];

    protected ?float $queueEmptiedAt = null;

    private bool $autoSetup;

    /**
     * Constructor.
     *
     * Available options:
     *
     * * table_name: name of the table
     * * connection: name of the Doctrine's entity manager
     * * queue_name: name of the queue
     * * redeliver_timeout: Timeout before redeliver messages still in handling state (i.e: delivered_at is not null and message is still in table). Default: 3600
     * * auto_setup: Whether the table should be created automatically during send / get. Default: true
     */
    public function __construct(
        protected array $configuration,
        protected DBALConnection $driverConnection,
    ) {
        $this->configuration = array_replace_recursive(static::DEFAULT_OPTIONS, $configuration);
        $this->autoSetup = $this->configuration['auto_setup'];
    }

    public function reset(): void
    {
        $this->queueEmptiedAt = null;
    }

    public function getConfiguration(): array
    {
        return $this->configuration;
    }

    public static function buildConfiguration(#[\SensitiveParameter] string $dsn, array $options = []): array
    {
        if (false === $params = parse_url($dsn)) {
            throw new InvalidArgumentException('The given Doctrine Messenger DSN is invalid.');
        }

        $query = [];
        if (isset($params['query'])) {
            parse_str($params['query'], $query);
        }

        $configuration = ['connection' => $params['host']];
        $configuration += $query + $options + static::DEFAULT_OPTIONS;

        $configuration['auto_setup'] = filter_var($configuration['auto_setup'], \FILTER_VALIDATE_BOOL);

        // check for extra keys in options
        $optionsExtraKeys = array_diff(array_keys($options), array_keys(static::DEFAULT_OPTIONS));
        if (0 < \count($optionsExtraKeys)) {
            throw new InvalidArgumentException(\sprintf('Unknown option found: [%s]. Allowed options are [%s].', implode(', ', $optionsExtraKeys), implode(', ', array_keys(static::DEFAULT_OPTIONS))));
        }

        // check for extra keys in options
        $queryExtraKeys = array_diff(array_keys($query), array_keys(static::DEFAULT_OPTIONS));
        if (0 < \count($queryExtraKeys)) {
            throw new InvalidArgumentException(\sprintf('Unknown option found in DSN: [%s]. Allowed options are [%s].', implode(', ', $queryExtraKeys), implode(', ', array_keys(static::DEFAULT_OPTIONS))));
        }

        return $configuration;
    }

    /**
     * @return string|null The inserted id. NULL is returned when the message was already sent and the current one is deduplicated.
     */
    public function send(string $body, array $headers, int $delay = 0, ?string $messageId = null): ?string
    {
        $deduplicate = $this->configuration['deduplicate'];
        if ($deduplicate) {
            if (!$messageId) {
                throw new \RuntimeException('The transport is configured with message deduplication. The message ID is required!');
            }

            if ($this->isMessageIndexed($messageId)) {
                return null;
            }
        }

        if (0 !== $delay) {
            throw new \RuntimeException('Delay is not supported!');
        }

        insert:
        if ($deduplicate) {
            $this->driverConnection->beginTransaction();
        }
        try {
            $now = new \DateTimeImmutable('UTC');

            if ($deduplicate) {
                $queryBuilder = $this->driverConnection->createQueryBuilder()
                    ->insert($this->configuration['index_table_name'])
                    ->values([
                        'id' => '?',
                        'created_at' => '?',
                    ]);

                $this->executeStatement($queryBuilder->getSQL(), [
                    $messageId,
                    $now,
                ], [
                    Types::STRING,
                    Types::DATETIME_IMMUTABLE,
                ]);
            }

            $insertSql = $this->driverConnection->createQueryBuilder()
                ->insert($this->configuration['table_name'])
                ->values([
                    'body' => '?',
                    'headers' => '?',
                    'created_at' => '?',
                ])->getSQL();

            $insertParams = [
                $body,
                json_encode($headers),
                $now,
            ];

            $insertTypes = [
                Types::STRING,
                Types::STRING,
                Types::DATETIME_IMMUTABLE,
            ];

            if ($this->driverConnection->getDatabasePlatform() instanceof PostgreSQLPlatform) {
                if (!$result = $this->driverConnection->fetchFirstColumn($insertSql.' RETURNING id', $insertParams, $insertTypes)[0] ?? null) {
                    throw new TransportException('no id was returned by PostgreSQL from RETURNING clause.');
                }

                $this->driverConnection->executeStatement('SELECT pg_notify(?, ?)', [$this->configuration['table_name'], 'tx_rx']);
            } else {
                $this->executeStatement($insertSql, $insertParams, $insertTypes);

                if (!$result = $this->driverConnection->lastInsertId()) {
                    throw new TransportException('lastInsertId() returned false, no id was returned.');
                }
            }

            if ($deduplicate) {
                $this->driverConnection->commit();
            }
        } catch (\Exception $exception) {
            if ($deduplicate) {
                $this->driverConnection->rollBack();
            }

            // handle setup after transaction is no longer open
            if ($this->autoSetup && $exception instanceof TableNotFoundException) {
                $this->setup();
                goto insert;
            }

            throw $exception;
        }

        return (string) $result;
    }

    public function update(int|string $id, string $body, array $headers): bool
    {
        $queryBuilder = $this->driverConnection->createQueryBuilder()
            ->update($this->configuration['table_name'])
            ->set('body', ':p_body')
            ->set('headers', ':p_headers')
            ->where('id = :p_id');

        return 1 === $this->executeStatement($queryBuilder->getSQL(), [
            'p_body' => $body,
            'p_headers' => json_encode($headers),
            'p_id' => $id,
        ]);
    }

    public function updateRetryCount(int|string $id, int $retryCount, ?string $errorDetails = null): bool
    {
        $queryBuilder = $this->driverConnection->createQueryBuilder()
            ->update($this->configuration['table_name'])
            ->set('retry_count', ':p_retry_count')
            ->set('error_details', ':p_error_details')
            ->where('id = :p_id');

        return 1 === $this->executeStatement($queryBuilder->getSQL(), [
            'p_retry_count' => $retryCount,
            'p_error_details' => $errorDetails,
            'p_id' => $id,
        ]);
    }

    /**
     * Returns a list of available messages (each as a decoded associative array), or null when the queue is empty.
     *
     * @param int $fetchSize Best-effort hint about how many messages to fetch in one call
     *
     * @return array<int, array<string, mixed>>|null
     */
    public function get(int $fetchSize = 1): ?array
    {
        $fetchSize = max(1, $fetchSize);

        $query = $this->createAvailableMessagesQueryBuilder()
            ->orderBy('m.id', 'ASC')
            ->setMaxResults($fetchSize);

        if (!$this->configuration['multiple_consumers']) {
            $doctrineEnvelopes = $this->executeQuery(
                $query->getSQL(),
                $query->getParameters(),
                $query->getParameterTypes()
            )->fetchAllAssociative();

            if ([] === $doctrineEnvelopes) {
                $this->queueEmptiedAt = microtime(true) * 1000;

                return null;
            }
            $this->queueEmptiedAt = null;

            return array_map($this->decodeEnvelopeHeaders(...), $doctrineEnvelopes);
        }

        get:
        $sql = $query->forUpdate(ConflictResolutionMode::SKIP_LOCKED)->getSQL();

        $this->driverConnection->beginTransaction();
        try {
            $doctrineEnvelopes = $this->executeQuery(
                $sql,
                $query->getParameters(),
                $query->getParameterTypes()
            )->fetchAllAssociative();

            if ([] === $doctrineEnvelopes) {
                $this->driverConnection->commit();
                $this->queueEmptiedAt = microtime(true) * 1000;

                return null;
            }
            $this->queueEmptiedAt = null;

            $doctrineEnvelopes = array_map($this->decodeEnvelopeHeaders(...), $doctrineEnvelopes);

            $now = new \DateTimeImmutable('UTC');
            $ids = array_column($doctrineEnvelopes, 'id');

            if (1 === \count($ids)) {
                $queryBuilder = $this->driverConnection->createQueryBuilder()
                    ->update($this->configuration['table_name'])
                    ->set('delivered_at', '?')
                    ->where('id = ?');
                $this->executeStatement($queryBuilder->getSQL(), [
                    $now,
                    $ids[0],
                ], [
                    Types::DATETIME_IMMUTABLE,
                ]);
            } else {
                $queryBuilder = $this->driverConnection->createQueryBuilder()
                    ->update($this->configuration['table_name'])
                    ->set('delivered_at', '?')
                    ->where('id IN (?)');
                $this->executeStatement($queryBuilder->getSQL(), [
                    $now,
                    $ids,
                ], [
                    Types::DATETIME_IMMUTABLE,
                    ArrayParameterType::STRING,
                ]);
            }

            $this->driverConnection->commit();

            return $doctrineEnvelopes;
        } catch (\Throwable $exception) {
            $this->driverConnection->rollBack();

            // handle setup after transaction is no longer open
            if ($this->autoSetup && $exception instanceof TableNotFoundException) {
                $this->setup();
                goto get;
            }

            throw $exception;
        }
    }

    public function ack(int|string $id, ?string $messageId = null): bool
    {
        return $this->markMessageAsProcessed($id, $messageId);
    }

    public function reject(int|string $id, ?string $messageId = null): bool
    {
        return $this->markMessageAsProcessed($id, $messageId);
    }

    public function setup(): void
    {
        $configuration = $this->driverConnection->getConfiguration();
        $assetFilter = $configuration->getSchemaAssetsFilter();
        $configuration->setSchemaAssetsFilter(function ($tableName) {
            if ($tableName instanceof NamedObject) {
                // DBAL 4.4+
                $tableName = $tableName->getObjectName()->toString();
            } elseif ($tableName instanceof AbstractAsset) {
                // DBAL < 4.4
                $tableName = $tableName->getName();
            }

            if (!\is_string($tableName)) {
                throw new \TypeError(\sprintf('The table name must be an instance of "%s" or a string ("%s" given).', AbstractAsset::class, get_debug_type($tableName)));
            }

            return $tableName === $this->configuration['table_name']
                || $tableName === $this->configuration['index_table_name'];
        });
        $this->updateSchema();
        $configuration->setSchemaAssetsFilter($assetFilter);
        $this->autoSetup = false;
    }

    public function getMessageCount(): int
    {
        $queryBuilder = $this->createAvailableMessagesQueryBuilder()
            ->select('COUNT(m.id) AS message_count')
            ->setMaxResults(1);

        $stmt = $this->executeQuery($queryBuilder->getSQL(), $queryBuilder->getParameters(), $queryBuilder->getParameterTypes());

        return $stmt->fetchOne();
    }

    public function findAll(?int $limit = null): array
    {
        $queryBuilder = $this->createAvailableMessagesQueryBuilder();

        if (null !== $limit) {
            $queryBuilder->setMaxResults($limit);
        }

        return array_map(
            $this->decodeEnvelopeHeaders(...),
            $this->executeQuery($queryBuilder->getSQL(), $queryBuilder->getParameters(), $queryBuilder->getParameterTypes())->fetchAllAssociative()
        );
    }

    public function find(mixed $id): ?array
    {
        $queryBuilder = $this->createQueryBuilder()
            ->where('m.id = ?');

        $stmt = $this->executeQuery($queryBuilder->getSQL(), [$id]);
        $data = $stmt->fetchAssociative();

        return false === $data ? null : $this->decodeEnvelopeHeaders($data);
    }

    public function configureSchema(Schema $schema, DBALConnection $forConnection, \Closure $isSameDatabase): void
    {
        $hasMainTable = $schema->hasTable($this->configuration['table_name']);
        $hasIndexTable = $schema->hasTable($this->configuration['index_table_name']);
        if ($hasMainTable && $hasIndexTable) {
            return;
        }

        if ($forConnection !== $this->driverConnection && !$isSameDatabase($this->executeStatement(...))) {
            return;
        }

        if (!$hasMainTable) {
            $this->addTableToSchema($schema);
        }

        if (!$hasIndexTable) {
            $this->addIndexTableToSchema($schema);
        }
    }

    public function getExtraSetupSqlForTable(Table $createdTable): array
    {
        return [];
    }

    public function getDriverConnection(): DBALConnection
    {
        return $this->driverConnection;
    }

    private function markMessageAsProcessed(int|string $id, ?string $messageId = null): bool
    {
        $deduplicate = $this->configuration['deduplicate'];
        try {
            $now = new \DateTimeImmutable();
            if ($deduplicate) {
                if (!$messageId) {
                    throw new \RuntimeException('The transport is configured with message deduplication. The message ID is required!');
                }
                $this->driverConnection->beginTransaction();
            }
            try {
                if ($deduplicate) {
                    $queryBuilder = $this->driverConnection->createQueryBuilder()
                        ->update($this->configuration['index_table_name'])
                        ->set('processed_at', ':p_processed_at')
                        ->where('id = :p_id');

                    $this->executeStatement($queryBuilder->getSQL(), [
                        'p_processed_at' => $now,
                        'p_id' => $messageId,
                    ], [
                        'p_processed_at' => Types::DATETIME_IMMUTABLE,
                        'p_id' => Types::STRING,
                    ]);
                }

                $result = $this->driverConnection->delete($this->configuration['table_name'], ['id' => $id]) > 0;

                if ($deduplicate) {
                    $this->driverConnection->commit();
                }
            } catch (\Exception $exception) {
                if ($deduplicate) {
                    $this->driverConnection->rollBack();
                }
                throw $exception;
            }
        } catch (DBALException $exception) {
            throw new TransportException($exception->getMessage(), 0, $exception);
        }

        return $result;
    }

    private function isMessageIndexed(string $messageId): bool
    {
        $tableName = $this->configuration['index_table_name'];
        $stmt = $this->executeQuery("select t.id from $tableName t where t.id = :pid", ['pid' => $messageId]);
        $indexedMessageId = $stmt->fetchOne();

        return $indexedMessageId === $messageId;
    }

    private function createAvailableMessagesQueryBuilder(): QueryBuilder
    {
        $qb = $this->createQueryBuilder();

        if (!$this->configuration['multiple_consumers']) {
            return $qb;
        }

        $now = new \DateTimeImmutable('UTC');
        $redeliverLimit = $now->modify(\sprintf('-%d seconds', $this->configuration['redeliver_timeout']));

        return $qb
            ->where('m.delivered_at is null OR m.delivered_at < ?')
            ->setParameters([
                $redeliverLimit,
            ], [
                Types::DATETIME_IMMUTABLE,
            ]);
    }

    private function createQueryBuilder(string $alias = 'm'): QueryBuilder
    {
        $queryBuilder = $this->driverConnection->createQueryBuilder()
            ->from($this->configuration['table_name'], $alias);

        return $queryBuilder->select($alias.'.*');
    }

    private function executeQuery(string $sql, array $parameters = [], array $types = []): DBALResult
    {
        try {
            return $this->driverConnection->executeQuery($sql, $parameters, $types);
        } catch (TableNotFoundException $e) {
            if (!$this->autoSetup || $this->driverConnection->isTransactionActive()) {
                throw $e;
            }
        }

        $this->setup();

        return $this->driverConnection->executeQuery($sql, $parameters, $types);
    }

    protected function executeStatement(string $sql, array $parameters = [], array $types = []): int|string
    {
        try {
            return $this->driverConnection->executeStatement($sql, $parameters, $types);
        } catch (TableNotFoundException $e) {
            if (!$this->autoSetup || $this->driverConnection->isTransactionActive()) {
                throw $e;
            }
        }

        $this->setup();

        return $this->driverConnection->executeStatement($sql, $parameters, $types);
    }

    private function getSchema(): Schema
    {
        $schema = new Schema([], [], $this->driverConnection->createSchemaManager()->createSchemaConfig());
        $this->addTableToSchema($schema);
        $this->addIndexTableToSchema($schema);

        return $schema;
    }

    private function addTableToSchema(Schema $schema): void
    {
        $table = $schema->createTable($this->configuration['table_name']);
        $table->addOption(self::TABLE_OPTION_NAME, $this->configuration['table_name']);
        $table->addColumn('id', Types::BIGINT, ['autoincrement' => true, 'notnull' => true]);
        $table->addColumn('body', Types::TEXT, ['notnull' => true]);
        $table->addColumn('headers', Types::TEXT, ['notnull' => true]);
        $table->addColumn('created_at', Types::DATETIME_IMMUTABLE, ['notnull' => true]);
        $table->addColumn('delivered_at', Types::DATETIME_IMMUTABLE, ['notnull' => false]);
        $table->addColumn('retry_count', Types::INTEGER, ['notnull' => true, 'default' => 0]);
        $table->addColumn('error_details', Types::TEXT, ['notnull' => false]);
        $table->addPrimaryKeyConstraint(new PrimaryKeyConstraint(null, [new UnqualifiedName(Identifier::unquoted('id'))], true));
        $table->addIndex(['delivered_at']);
    }

    private function addIndexTableToSchema(Schema $schema): void
    {
        if (!$this->configuration['deduplicate']) {
            return;
        }
        $table = $schema->createTable($this->configuration['index_table_name']);
        $table->addOption(self::TABLE_OPTION_NAME, $this->configuration['index_table_name']);
        $table->addColumn('id', Types::GUID, ['notnull' => true]);
        $table->addColumn('created_at', Types::DATETIME_IMMUTABLE, ['notnull' => true]);
        $table->addColumn('processed_at', Types::DATETIME_IMMUTABLE, ['notnull' => false]);
        $table->addPrimaryKeyConstraint(new PrimaryKeyConstraint(null, [new UnqualifiedName(Identifier::unquoted('id'))], true));
        $table->addIndex(['created_at']);
        $table->addIndex(['processed_at']);
    }

    private function decodeEnvelopeHeaders(array $doctrineEnvelope): array
    {
        $doctrineEnvelope['headers'] = json_decode($doctrineEnvelope['headers'], true);

        return $doctrineEnvelope;
    }

    private function updateSchema(): void
    {
        $schemaManager = $this->driverConnection->createSchemaManager();
        $schemaDiff = $schemaManager->createComparator()
            ->compareSchemas($schemaManager->introspectSchema(), $this->getSchema());
        $platform = $this->driverConnection->getDatabasePlatform();

        if ($platform->supportsSchemas()) {
            foreach ($schemaDiff->getCreatedSchemas() as $schema) {
                $this->driverConnection->executeStatement($platform->getCreateSchemaSQL($schema));
            }
        }

        if ($platform->supportsSequences()) {
            foreach ($schemaDiff->getAlteredSequences() as $sequence) {
                $this->driverConnection->executeStatement($platform->getAlterSequenceSQL($sequence));
            }

            foreach ($schemaDiff->getCreatedSequences() as $sequence) {
                $this->driverConnection->executeStatement($platform->getCreateSequenceSQL($sequence));
            }
        }

        foreach ($platform->getCreateTablesSQL($schemaDiff->getCreatedTables()) as $sql) {
            $this->driverConnection->executeStatement($sql);
        }

        foreach ($schemaDiff->getAlteredTables() as $tableDiff) {
            foreach ($platform->getAlterTableSQL($tableDiff) as $sql) {
                $this->driverConnection->executeStatement($sql);
            }
        }
    }
}
