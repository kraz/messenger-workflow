<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Integration\Doctrine;

use Doctrine\DBAL\Connection as DBALConnection;
use Doctrine\DBAL\DriverManager;
use Kraz\MessengerWorkflow\Messenger\Doctrine\Connection;

final class ConnectionSqliteTest extends AbstractConnectionTestCase
{
    private ?DBALConnection $dbal = null;

    protected function createDbalConnection(): DBALConnection
    {
        return $this->dbal ??= DriverManager::getConnection([
            'driver' => 'pdo_sqlite',
            'memory' => true,
        ]);
    }

    protected function createConnection(array $options = []): Connection
    {
        return new Connection($this->baseOptions($options), $this->createDbalConnection());
    }
}
