<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Infrastructure\Doctrine;

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

/**
 * Message-table access for the workflow outbox/inbox transports (ported from the
 * original package). FIFO by auto-increment id; optional competing consumers via
 * FOR UPDATE SKIP LOCKED + redelivery timeout; optional deduplication by message
 * UUID through a companion index table.
 *
 * @phpstan-type DoctrineEnvelope array{id: int|string, body: string, headers: array<array-key, mixed>, retry_count: int}
 */
class Connection implements ResetInterface
{
    protected const string TABLE_OPTION_NAME = '_symfony_messenger_table_name';

    protected const array DEFAULT_OPTIONS = [
        'table_name' => 'messenger_messages',
        'index_table_name' => 'messenger_messages_index',
        'redeliver_timeout' => 5 * 60,
        'multiple_consumers' => false,
        'strict_order' => false,
        'deduplicate' => false,
        'auto_setup' => false,
    ];

    /**
     * Options which are normalized to int/bool (DSN query values arrive as strings).
     *
     * @var list<string>
     */
    protected const array INT_OPTIONS = ['redeliver_timeout', 'check_delayed_interval', 'get_notify_timeout'];

    /**
     * @var list<string>
     */
    protected const array BOOL_OPTIONS = ['multiple_consumers', 'strict_order', 'deduplicate', 'auto_setup'];

    protected ?float $queueEmptiedAt = null;

    private bool $autoSetup;

    protected readonly string $tableName;
    protected readonly string $indexTableName;
    protected readonly int $redeliverTimeout;
    protected readonly bool $multipleConsumers;
    protected readonly bool $strictOrder;
    protected readonly bool $deduplicate;

    /**
     * @param array<array-key, mixed> $configuration
     */
    public function __construct(
        protected array $configuration,
        protected DBALConnection $driverConnection,
    ) {
        $this->configuration = array_replace_recursive(static::DEFAULT_OPTIONS, $configuration);

        $this->tableName = \is_string($this->configuration['table_name']) ? $this->configuration['table_name'] : throw new InvalidArgumentException('The "table_name" option must be a string.');
        $this->indexTableName = \is_string($this->configuration['index_table_name']) ? $this->configuration['index_table_name'] : throw new InvalidArgumentException('The "index_table_name" option must be a string.');
        $this->redeliverTimeout = (int) (is_numeric($this->configuration['redeliver_timeout']) ? $this->configuration['redeliver_timeout'] : throw new InvalidArgumentException('The "redeliver_timeout" option must be an integer.'));
        $this->multipleConsumers = (bool) $this->configuration['multiple_consumers'];
        $this->strictOrder = (bool) $this->configuration['strict_order'];
        $this->deduplicate = (bool) $this->configuration['deduplicate'];
        $this->autoSetup = (bool) $this->configuration['auto_setup'];

        // Strict ordering requires a single FIFO consumer — competing consumers
        // (SKIP LOCKED) would reorder messages by design.
        if ($this->strictOrder && $this->multipleConsumers) {
            throw new InvalidArgumentException('The "strict_order" and "multiple_consumers" options are mutually exclusive: ordered delivery requires a single FIFO consumer.');
        }
    }

    public function isStrictOrder(): bool
    {
        return $this->strictOrder;
    }

    public function reset(): void
    {
        $this->queueEmptiedAt = null;
    }

    /**
     * @return array<array-key, mixed>
     */
    public function getConfiguration(): array
    {
        return $this->configuration;
    }

    public function getTableName(): string
    {
        return $this->tableName;
    }

    public function isDeduplicating(): bool
    {
        return $this->deduplicate;
    }

    /**
     * @param array<array-key, mixed> $options
     *
     * @return array<array-key, mixed>
     */
    public static function buildConfiguration(#[\SensitiveParameter] string $dsn, array $options = []): array
    {
        if (false === $params = parse_url($dsn)) {
            throw new InvalidArgumentException('The given Doctrine Messenger DSN is invalid.');
        }

        $query = [];
        if (isset($params['query'])) {
            parse_str($params['query'], $query);
        }

        $configuration = ['connection' => $params['host'] ?? null];
        $configuration += $query + $options + static::DEFAULT_OPTIONS;

        foreach (static::BOOL_OPTIONS as $boolOption) {
            if (\array_key_exists($boolOption, $configuration)) {
                $configuration[$boolOption] = filter_var($configuration[$boolOption], \FILTER_VALIDATE_BOOL);
            }
        }
        foreach (static::INT_OPTIONS as $intOption) {
            if (\array_key_exists($intOption, $configuration) && is_numeric($configuration[$intOption])) {
                $configuration[$intOption] = (int) $configuration[$intOption];
            }
        }

        // check for extra keys in options
        $optionsExtraKeys = array_diff(array_keys($options), array_keys(static::DEFAULT_OPTIONS));
        if (0 < \count($optionsExtraKeys)) {
            throw new InvalidArgumentException(\sprintf('Unknown option found: [%s]. Allowed options are [%s].', implode(', ', $optionsExtraKeys), implode(', ', array_keys(static::DEFAULT_OPTIONS))));
        }

        // check for extra keys in DSN query
        $queryExtraKeys = array_diff(array_keys($query), array_keys(static::DEFAULT_OPTIONS));
        if (0 < \count($queryExtraKeys)) {
            throw new InvalidArgumentException(\sprintf('Unknown option found in DSN: [%s]. Allowed options are [%s].', implode(', ', $queryExtraKeys), implode(', ', array_keys(static::DEFAULT_OPTIONS))));
        }

        return $configuration;
    }

    /**
     * @param array<array-key, mixed> $headers
     *
     * @return string|null The inserted id. NULL is returned when the message was already sent and the current one is deduplicated.
     */
    public function send(string $body, array $headers, int $delay = 0, ?string $messageId = null): ?string
    {
        if ($this->deduplicate) {
            if (null === $messageId || '' === $messageId) {
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
        if ($this->deduplicate) {
            $this->driverConnection->beginTransaction();
        }
        try {
            $now = new \DateTimeImmutable('UTC');

            if ($this->deduplicate) {
                $queryBuilder = $this->driverConnection->createQueryBuilder()
                    ->insert($this->indexTableName)
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
                ->insert($this->tableName)
                ->values([
                    'body' => '?',
                    'headers' => '?',
                    'created_at' => '?',
                ])->getSQL();

            $insertParams = [
                $body,
                json_encode($headers, \JSON_THROW_ON_ERROR),
                $now,
            ];

            $insertTypes = [
                Types::STRING,
                Types::STRING,
                Types::DATETIME_IMMUTABLE,
            ];

            if ($this->driverConnection->getDatabasePlatform() instanceof PostgreSQLPlatform) {
                $returned = $this->driverConnection->fetchFirstColumn($insertSql.' RETURNING id', $insertParams, $insertTypes)[0] ?? null;
                if (!is_numeric($returned) && !\is_string($returned)) {
                    throw new TransportException('no id was returned by PostgreSQL from RETURNING clause.');
                }
                $result = $returned;

                $this->driverConnection->executeStatement('SELECT pg_notify(?, ?)', [$this->tableName, 'tx_rx']);
            } else {
                $this->executeStatement($insertSql, $insertParams, $insertTypes);

                $result = $this->driverConnection->lastInsertId();
            }

            if ($this->deduplicate) {
                $this->driverConnection->commit();
            }
        } catch (\Exception $exception) {
            if ($this->deduplicate) {
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

    /**
     * Rewrites a stored message in place (retry redeliveries of inbox messages). The
     * delivered_at marker is cleared so the row becomes consumable again immediately —
     * retry delays are not supported by these tables (no available_at column).
     *
     * @param array<array-key, mixed> $headers
     */
    public function update(int|string $id, string $body, array $headers): bool
    {
        $queryBuilder = $this->driverConnection->createQueryBuilder()
            ->update($this->tableName)
            ->set('body', ':p_body')
            ->set('headers', ':p_headers')
            ->set('delivered_at', ':p_delivered_at')
            ->where('id = :p_id');

        return 1 === $this->executeStatement($queryBuilder->getSQL(), [
            'p_body' => $body,
            'p_headers' => json_encode($headers, \JSON_THROW_ON_ERROR),
            'p_delivered_at' => null,
            'p_id' => $id,
        ]);
    }

    public function updateRetryCount(int|string $id, int $retryCount, ?string $errorDetails = null): bool
    {
        $queryBuilder = $this->driverConnection->createQueryBuilder()
            ->update($this->tableName)
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
     * @return list<DoctrineEnvelope>|null
     */
    public function get(int $fetchSize = 1): ?array
    {
        $fetchSize = max(1, $fetchSize);

        $query = $this->createAvailableMessagesQueryBuilder()
            ->orderBy('m.id', 'ASC')
            ->setMaxResults($fetchSize);

        if (!$this->multipleConsumers) {
            $doctrineEnvelopes = $this->executeQuery(
                $query->getSQL(),
                $query->getParameters(),
                $query->getParameterTypes(),
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
                $query->getParameterTypes(),
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
                    ->update($this->tableName)
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
                    ->update($this->tableName)
                    ->set('delivered_at', '?')
                    ->where('id IN (?)');
                $this->executeStatement($queryBuilder->getSQL(), [
                    $now,
                    array_map(strval(...), $ids),
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

    /**
     * Refreshes the delivered_at marker of an in-flight message so a long-running
     * handler is not considered stuck and redelivered to a competing consumer after
     * redeliver_timeout (Worker --keepalive support).
     */
    public function keepalive(int|string $id, ?int $seconds = null): void
    {
        // A keepalive interval above the redeliver timeout cannot prevent redelivery.
        if (null !== $seconds && $this->redeliverTimeout < $seconds) {
            throw new TransportException(\sprintf('The redeliver_timeout (%ds) cannot be smaller than the keepalive interval (%ds).', $this->redeliverTimeout, $seconds));
        }

        // No transaction: the keepalive runs from a SIGALRM handler that may interrupt
        // the connection between the driver call and the nesting-level bookkeeping,
        // where beginTransaction() would corrupt the nesting state.
        $queryBuilder = $this->driverConnection->createQueryBuilder()
            ->update($this->tableName)
            ->set('delivered_at', '?')
            ->where('id = ?');
        $this->executeStatement($queryBuilder->getSQL(), [
            new \DateTimeImmutable('UTC'),
            $id,
        ], [
            Types::DATETIME_IMMUTABLE,
        ]);
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
        $configuration->setSchemaAssetsFilter(function (mixed $tableName): bool {
            if ($tableName instanceof NamedObject) {
                $tableName = $tableName->getObjectName()->toString();
            } elseif ($tableName instanceof AbstractAsset) {
                $tableName = $tableName->getName();
            }

            if (!\is_string($tableName)) {
                throw new \TypeError(\sprintf('The table name must be an instance of "%s" or a string ("%s" given).', AbstractAsset::class, get_debug_type($tableName)));
            }

            return $tableName === $this->tableName || $tableName === $this->indexTableName;
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

        $count = $this->executeQuery($queryBuilder->getSQL(), $queryBuilder->getParameters(), $queryBuilder->getParameterTypes())->fetchOne();

        return is_numeric($count) ? (int) $count : 0;
    }

    /**
     * @return list<DoctrineEnvelope>
     */
    public function findAll(?int $limit = null): array
    {
        $queryBuilder = $this->createAvailableMessagesQueryBuilder();

        if (null !== $limit) {
            $queryBuilder->setMaxResults($limit);
        }

        return array_map(
            $this->decodeEnvelopeHeaders(...),
            $this->executeQuery($queryBuilder->getSQL(), $queryBuilder->getParameters(), $queryBuilder->getParameterTypes())->fetchAllAssociative(),
        );
    }

    /**
     * @return DoctrineEnvelope|null
     */
    public function find(mixed $id): ?array
    {
        $queryBuilder = $this->createQueryBuilder()
            ->where('m.id = ?');

        $data = $this->executeQuery($queryBuilder->getSQL(), [$id])->fetchAssociative();

        return false === $data ? null : $this->decodeEnvelopeHeaders($data);
    }

    /**
     * Whether any stored message contains the given value in its serialized form
     * (headers hold the stamps with the JSON serializer, the body with the PHP
     * serializer). Used by the command bus deadlock guard to detect a task id still
     * parked in this outbox; runs on the transport's own connection, so it sees rows
     * of the ambient (uncommitted) transaction.
     */
    public function hasMessageContaining(string $value): bool
    {
        $queryBuilder = $this->createQueryBuilder()
            ->select('m.id')
            ->where('m.headers LIKE :needle OR m.body LIKE :needle')
            ->setMaxResults(1);

        return false !== $this->executeQuery(
            $queryBuilder->getSQL(),
            ['needle' => '%'.addcslashes($value, '%_\\').'%'],
        )->fetchOne();
    }

    public function configureSchema(Schema $schema, DBALConnection $forConnection, \Closure $isSameDatabase): void
    {
        $hasMainTable = $schema->hasTable($this->tableName);
        $hasIndexTable = $schema->hasTable($this->indexTableName);
        if ($hasMainTable && ($hasIndexTable || !$this->deduplicate)) {
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

    /**
     * @return list<string>
     */
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
        try {
            $now = new \DateTimeImmutable();
            if ($this->deduplicate) {
                if (null === $messageId || '' === $messageId) {
                    throw new \RuntimeException('The transport is configured with message deduplication. The message ID is required!');
                }
                $this->driverConnection->beginTransaction();
            }
            try {
                if ($this->deduplicate) {
                    $queryBuilder = $this->driverConnection->createQueryBuilder()
                        ->update($this->indexTableName)
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

                $result = $this->driverConnection->delete($this->tableName, ['id' => $id]) > 0;

                if ($this->deduplicate) {
                    $this->driverConnection->commit();
                }
            } catch (\Exception $exception) {
                if ($this->deduplicate) {
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
        $tableName = $this->indexTableName;
        $indexedMessageId = $this->executeQuery("select t.id from $tableName t where t.id = :pid", ['pid' => $messageId])->fetchOne();

        return $indexedMessageId === $messageId;
    }

    private function createAvailableMessagesQueryBuilder(): QueryBuilder
    {
        $qb = $this->createQueryBuilder();

        if (!$this->multipleConsumers) {
            return $qb;
        }

        $now = new \DateTimeImmutable('UTC');
        $redeliverLimit = $now->modify(\sprintf('-%d seconds', $this->redeliverTimeout));

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
        return $this->driverConnection->createQueryBuilder()
            ->from($this->tableName, $alias)
            ->select($alias.'.*');
    }

    /**
     * @param array<int<0, max>|string, mixed>                                                                              $parameters
     * @param array<int<0, max>|string, \Doctrine\DBAL\ArrayParameterType|\Doctrine\DBAL\ParameterType|\Doctrine\DBAL\Types\Type|string> $types
     */
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

    /**
     * @param array<int<0, max>|string, mixed>                                                                              $parameters
     * @param array<int<0, max>|string, \Doctrine\DBAL\ArrayParameterType|\Doctrine\DBAL\ParameterType|\Doctrine\DBAL\Types\Type|string> $types
     */
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
        $table = $schema->createTable($this->tableName);
        $table->addOption(self::TABLE_OPTION_NAME, $this->tableName);
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
        if (!$this->deduplicate) {
            return;
        }
        $table = $schema->createTable($this->indexTableName);
        $table->addOption(self::TABLE_OPTION_NAME, $this->indexTableName);
        $table->addColumn('id', Types::GUID, ['notnull' => true]);
        $table->addColumn('created_at', Types::DATETIME_IMMUTABLE, ['notnull' => true]);
        $table->addColumn('processed_at', Types::DATETIME_IMMUTABLE, ['notnull' => false]);
        $table->addPrimaryKeyConstraint(new PrimaryKeyConstraint(null, [new UnqualifiedName(Identifier::unquoted('id'))], true));
        $table->addIndex(['created_at']);
        $table->addIndex(['processed_at']);
    }

    /**
     * @param array<array-key, mixed> $doctrineEnvelope
     *
     * @return DoctrineEnvelope
     */
    private function decodeEnvelopeHeaders(array $doctrineEnvelope): array
    {
        $id = $doctrineEnvelope['id'] ?? null;
        $body = $doctrineEnvelope['body'] ?? null;
        if ((!\is_int($id) && !\is_string($id)) || !\is_string($body)) {
            throw new TransportException('Unexpected message row shape.');
        }

        $headers = \is_string($doctrineEnvelope['headers'] ?? null) ? json_decode($doctrineEnvelope['headers'], true) : null;

        return [
            'id' => $id,
            'body' => $body,
            'headers' => \is_array($headers) ? $headers : [],
            'retry_count' => is_numeric($doctrineEnvelope['retry_count'] ?? null) ? (int) $doctrineEnvelope['retry_count'] : 0,
        ];
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
