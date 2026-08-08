<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Integration\Doctrine;

use Doctrine\DBAL\Connection as DBALConnection;
use Kraz\MessengerWorkflow\Infrastructure\Doctrine\Connection;
use Kraz\MessengerWorkflow\Infrastructure\Doctrine\PostgreSqlConnection;
use Kraz\MessengerWorkflow\Tests\Support\PostgresDbal;
use PHPUnit\Framework\Attributes\Group;
use PHPUnit\Framework\Attributes\RequiresPhpExtension;
use Symfony\Component\Messenger\Exception\TransportException;

/**
 * Spec: PostgreSQL infrastructure; LISTEN/NOTIFY push, SKIP LOCKED multi-consumer
 * fetching and redelivery of stuck messages. Ported from the original package.
 */
#[Group('postgres')]
#[RequiresPhpExtension('pdo_pgsql')]
final class ConnectionPostgreSqlTest extends AbstractConnectionTestCase
{
    /** @var list<DBALConnection> */
    private array $dbalConnections = [];

    protected function createDbalConnection(): DBALConnection
    {
        try {
            $connection = PostgresDbal::createConnection();
            $connection->executeQuery('SELECT 1');
        } catch (\Throwable $e) {
            self::markTestSkipped('PostgreSQL is not reachable: '.$e->getMessage());
        }

        $this->dbalConnections[] = $connection;

        return $connection;
    }

    protected function createConnection(array $options = []): Connection
    {
        // Base Connection: no LISTEN/NOTIFY blocking involved, plain SQL behavior
        return new Connection($this->baseOptions($options), $this->dbalConnections[0] ?? $this->createDbalConnection());
    }

    /**
     * @param array<string, mixed> $options
     */
    private function createPgConnection(array $options = [], ?DBALConnection $dbal = null): PostgreSqlConnection
    {
        $options = $this->baseOptions($options) + [
            // short listener wait and fallback re-poll boundary for tests
            'get_notify_timeout' => 100,
            'check_delayed_interval' => 200,
        ];

        return new PostgreSqlConnection($options, $dbal ?? $this->createDbalConnection());
    }

    protected function tearDown(): void
    {
        foreach (\array_slice($this->dbalConnections, 0, 1) as $dbal) {
            foreach ([$this->tableName, $this->indexTableName] as $table) {
                $dbal->executeStatement(\sprintf('DROP TABLE IF EXISTS "%s"', $table));
            }
        }
        $this->dbalConnections = [];
    }

    public function testSendNotifiesListenersViaPgNotify(): void
    {
        $sender = $this->createPgConnection();
        $sender->setup();

        // separate database session listening on the table channel
        $listener = $this->createPgConnection([], $this->createDbalConnection());
        $listener->listen();

        $sender->send('body', []);

        self::assertTrue($listener->waitForNotify(2000), 'Expected a NOTIFY after outbox insert');
    }

    public function testGetFallbackDoesNotBlockWaitingForNotify(): void
    {
        // get() is called for a single receiver of a worker that may consume several
        // transports: a blocking wait here would starve the sibling receivers and
        // override the worker's polling rate (--sleep) and deadline (--time-limit).
        // All blocking belongs to PostgreSqlNotifyOnIdleListener.
        $connection = $this->createPgConnection([
            'get_notify_timeout' => 60000,
            'check_delayed_interval' => 60000,
        ]);
        $connection->setup();

        self::assertNull($connection->get(), 'The queue starts empty');

        // second call on the emptied queue takes the LISTEN fallback path
        $start = microtime(true);
        self::assertNull($connection->get());
        self::assertLessThan(0.5, microtime(true) - $start, 'The fallback in get() must not wait for a NOTIFY');
    }

    public function testGetFallbackCollectsAlreadyArrivedNotifications(): void
    {
        // A NOTIFY that arrived between two get() calls must trigger a fetch even
        // before the check_delayed_interval re-poll boundary is reached.
        $connection = $this->createPgConnection([
            'get_notify_timeout' => 60000,
            'check_delayed_interval' => 60000,
        ]);
        $connection->setup();

        $initialFetch = $connection->get();
        self::assertNull($initialFetch, 'The queue starts empty');
        $fallbackFetch = $connection->get();
        self::assertNull($fallbackFetch, 'The fallback registers the LISTEN');

        $sender = $this->createPgConnection([], $this->createDbalConnection());
        $sender->send('body', []);

        // give the notification time to reach the consuming session
        usleep(100_000);

        $batch = $connection->get();
        self::assertNotNull($batch, 'A pending NOTIFY must trigger a fetch without waiting for the re-poll boundary');
        self::assertSame('body', $batch[0]['body']);
    }

    public function testWaitForNotifyBlocksForTheFullTimeout(): void
    {
        // The full timeout must reach getNotify() on the listener path: the no-wait
        // rule applies to the fallback in get() only, never to waitForNotify().
        $connection = $this->createPgConnection();
        $connection->setup();

        $start = microtime(true);
        self::assertFalse($connection->waitForNotify(300), 'No NOTIFY is expected on an idle queue');
        self::assertGreaterThan(0.25, microtime(true) - $start, 'waitForNotify() must wait the full timeout');
    }

    public function testMultipleConsumersLockAndRedeliverMessages(): void
    {
        // Spec: command inbox supports multiple concurrent consumers (SKIP LOCKED),
        // messages stuck in "delivered" state are redelivered after redeliver_timeout.
        $options = ['multiple_consumers' => true, 'redeliver_timeout' => 1];

        $consumerA = $this->createPgConnection($options);
        $consumerA->listen(false); // disable the blocking LISTEN fallback in get()
        $consumerA->setup();

        $consumerB = $this->createPgConnection($options, $this->createDbalConnection());
        $consumerB->listen(false);

        $consumerA->send('body', []);

        $firstFetch = $consumerA->get();
        self::assertNotNull($firstFetch);
        self::assertCount(1, $firstFetch);

        // delivered but not acked: hidden from the other consumer
        $hidden = $consumerB->get();
        self::assertNull($hidden);

        // after redeliver_timeout the message becomes visible again
        // (sleep > timeout + 1s: the delivered_at comparison is truncated to whole seconds)
        usleep(2_300_000);
        $redelivered = $consumerB->get();
        self::assertNotNull($redelivered);
        self::assertSame('body', $redelivered[0]['body']);
    }

    public function testCompetingConsumersSkipDelayedRowsInsteadOfBlocking(): void
    {
        // No ordering guarantee in competing-consumer mode: a row in retry backoff
        // is skipped by the SQL availability filter, successors are delivered.
        $connection = $this->createConnection(['multiple_consumers' => true]);
        $connection->setup();
        $firstId = $connection->send('b1', []);
        $connection->send('b2', []);
        self::assertNotNull($firstId);

        $connection->update($firstId, 'b1', [], new \DateTimeImmutable('+1 hour', new \DateTimeZone('UTC')));
        self::assertSame(1, $connection->getMessageCount(), 'The available-message count excludes rows in backoff');

        $batch = $connection->get(10);
        self::assertNotNull($batch);
        self::assertSame(['b2'], array_column($batch, 'body'), 'The delayed row is skipped, not blocking');
    }

    public function testKeepaliveExtendsDeliveryOfALongRunningHandler(): void
    {
        // Spec: a handler running longer than redeliver_timeout must not
        // have its message stolen by a competing consumer while the worker sends
        // keepalives (messenger:consume --keepalive).
        $options = ['multiple_consumers' => true, 'redeliver_timeout' => 1];

        $consumerA = $this->createPgConnection($options);
        $consumerA->listen(false);
        $consumerA->setup();

        $consumerB = $this->createPgConnection($options, $this->createDbalConnection());
        $consumerB->listen(false);

        $consumerA->send('body', []);
        $batch = $consumerA->get();
        self::assertNotNull($batch);
        $id = $batch[0]['id'];

        // "Handling" outlives the no-keepalive redelivery horizon (2.3 s, see the
        // redelivery test above) — periodic keepalives refresh delivered_at.
        for ($i = 0; $i < 3; ++$i) {
            usleep(800_000);
            $consumerA->keepalive($id);
            self::assertNull($consumerB->get(), 'The kept-alive in-flight row must stay invisible to competing consumers');
        }

        // Without further keepalives the row is redelivered normally.
        usleep(2_300_000);
        $redelivered = $consumerB->get();
        self::assertNotNull($redelivered);
        self::assertSame('body', $redelivered[0]['body']);
    }

    public function testKeepaliveRejectsAnIntervalAboveTheRedeliverTimeout(): void
    {
        $connection = $this->createPgConnection(['redeliver_timeout' => 1]);
        $connection->setup();

        $this->expectException(TransportException::class);
        $this->expectExceptionMessageMatches('/redeliver_timeout \(1s\) cannot be smaller/');

        $connection->keepalive('1', 5);
    }

    public function testAckedMessageIsNotRedelivered(): void
    {
        $options = ['multiple_consumers' => true, 'redeliver_timeout' => 1];
        $consumer = $this->createPgConnection($options);
        $consumer->listen(false);
        $consumer->setup();

        $consumer->send('body', []);
        $batch = $consumer->get();
        self::assertNotNull($batch);
        $consumer->ack($batch[0]['id']);

        usleep(2_300_000);
        self::assertNull($consumer->get());
    }
}
