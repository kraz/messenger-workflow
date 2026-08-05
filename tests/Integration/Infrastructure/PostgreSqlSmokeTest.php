<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Integration\Infrastructure;

use Kraz\MessengerWorkflow\Tests\Support\Infra;
use PHPUnit\Framework\Attributes\Group;
use PHPUnit\Framework\Attributes\RequiresPhpExtension;
use PHPUnit\Framework\TestCase;

#[Group('postgres')]
#[RequiresPhpExtension('pdo_pgsql')]
final class PostgreSqlSmokeTest extends TestCase
{
    public function testConnectAndQuery(): void
    {
        $pdo = Infra::requirePostgres();

        $statement = $pdo->query('SELECT 1');
        self::assertNotFalse($statement);
        self::assertSame(1, (int) $statement->fetchColumn());
    }

    public function testListenNotifyIsAvailable(): void
    {
        $pdo = Infra::requirePostgres();

        // LISTEN/NOTIFY is the wake-up mechanism for outbox/inbox workers.
        $pdo->exec('LISTEN mwf_smoke');
        $pdo->exec("NOTIFY mwf_smoke, 'ping'");
        $pdo->exec('UNLISTEN mwf_smoke');

        $this->addToAssertionCount(1);
    }
}
