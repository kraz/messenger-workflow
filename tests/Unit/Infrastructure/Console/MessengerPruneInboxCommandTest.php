<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Unit\Infrastructure\Console;

use Doctrine\DBAL\Connection as DBALConnection;
use Doctrine\DBAL\DriverManager;
use Doctrine\DBAL\Types\Types;
use Kraz\MessengerWorkflow\Infrastructure\Console\MessengerPruneInboxCommand;
use Kraz\MessengerWorkflow\Infrastructure\Doctrine\Connection;
use Kraz\MessengerWorkflow\Infrastructure\Doctrine\Inbox\InboxTransport;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Tester\CommandTester;
use Symfony\Component\DependencyInjection\ServiceLocator;
use Symfony\Component\Messenger\Transport\InMemory\InMemoryTransport;
use Symfony\Component\Messenger\Transport\Serialization\PhpSerializer;
use Symfony\Component\Uid\Uuid;

/**
 * Spec: messenger:prune-inbox deletes the processed dedup index entries of the inbox
 * transports older than a retention counted in whole days (at least one) — pending
 * entries and entries inside the retention keep deduplicating.
 */
final class MessengerPruneInboxCommandTest extends TestCase
{
    private DBALConnection $dbal;

    /**
     * @var array<string, Connection>
     */
    private array $connections = [];

    protected function setUp(): void
    {
        $this->dbal = DriverManager::getConnection(['driver' => 'pdo_sqlite', 'memory' => true]);
        foreach (['commands_inbox' => true, 'events_inbox' => true, 'plain_inbox' => false] as $name => $deduplicate) {
            $this->connections[$name] = new Connection([
                'table_name' => $name,
                'index_table_name' => $name.'_index',
                'deduplicate' => $deduplicate,
            ], $this->dbal);
            $this->connections[$name]->setup();
        }
    }

    private function tester(): CommandTester
    {
        $transports = ['commands' => static fn (): InMemoryTransport => new InMemoryTransport()];
        foreach ($this->connections as $name => $connection) {
            $transports[$name] = static fn (): InboxTransport => new InboxTransport($connection, new PhpSerializer());
        }

        return new CommandTester(new MessengerPruneInboxCommand(new ServiceLocator($transports), array_keys($transports)));
    }

    public function testPrunesEveryInboxTransportPastTheDefaultRetention(): void
    {
        $this->processedEntry('commands_inbox', '-8 days');
        $keptCommand = $this->processedEntry('commands_inbox', '-6 days');
        $this->processedEntry('events_inbox', '-30 days');
        $this->processedEntry('events_inbox', '-30 days');

        $tester = $this->tester();
        self::assertSame(Command::SUCCESS, $tester->execute([]));

        self::assertSame([$keptCommand], $this->indexedIds('commands_inbox'));
        self::assertSame([], $this->indexedIds('events_inbox'));
        $display = $tester->getDisplay();
        self::assertStringContainsString('commands_inbox_index', $display);
        self::assertStringContainsString('no deduplication', $display);
        self::assertStringContainsString('Deleted 3 processed inbox entries', $display);
        self::assertStringContainsString('retention: 7 days', $display);
        self::assertDoesNotMatchRegularExpression('/^\s*commands\s/m', $display, 'Non-inbox transports are skipped');
    }

    public function testRetentionAndTransportsAreSelectable(): void
    {
        $this->processedEntry('commands_inbox', '-3 days');
        $keptEvent = $this->processedEntry('events_inbox', '-3 days');

        $tester = $this->tester();
        self::assertSame(Command::SUCCESS, $tester->execute(['transports' => ['commands_inbox'], '--retention-days' => '2']));

        self::assertSame([], $this->indexedIds('commands_inbox'));
        self::assertSame([$keptEvent], $this->indexedIds('events_inbox'));
    }

    public function testDryRunOnlyCounts(): void
    {
        $old = $this->processedEntry('commands_inbox', '-10 days');

        $tester = $this->tester();
        self::assertSame(Command::SUCCESS, $tester->execute(['--dry-run' => true]));

        self::assertSame([$old], $this->indexedIds('commands_inbox'));
        self::assertStringContainsString('Would delete 1 processed inbox entry', $tester->getDisplay());
    }

    public function testPendingEntriesAreNeverPruned(): void
    {
        $pending = (string) Uuid::v7();
        self::assertNotNull($this->connections['commands_inbox']->send('body', [], 0, $pending));
        $this->dbal->executeStatement('UPDATE commands_inbox_index SET created_at = ?', [new \DateTimeImmutable('-90 days', new \DateTimeZone('UTC'))], [Types::DATETIME_IMMUTABLE]);

        self::assertSame(Command::SUCCESS, $this->tester()->execute(['--retention-days' => '1']));

        self::assertSame([$pending], $this->indexedIds('commands_inbox'));
    }

    /**
     * @return iterable<string, array{string}>
     */
    public static function invalidRetentions(): iterable
    {
        yield 'zero days' => ['0'];
        yield 'negative' => ['-7'];
        yield 'minutes' => ['30m'];
        yield 'fraction of a day' => ['0.5'];
    }

    #[DataProvider('invalidRetentions')]
    public function testRejectsARetentionBelowOneDay(string $retention): void
    {
        $old = $this->processedEntry('commands_inbox', '-10 days');

        $tester = $this->tester();
        self::assertSame(Command::INVALID, $tester->execute(['--retention-days' => $retention]));

        self::assertSame([$old], $this->indexedIds('commands_inbox'));
        self::assertStringContainsString('at least 1', $tester->getDisplay());
    }

    public function testRejectsAnInvalidBatchSize(): void
    {
        self::assertSame(Command::INVALID, $this->tester()->execute(['--batch-size' => '0']));
    }

    public function testFailsOnAnUnknownOrNonInboxTransport(): void
    {
        $old = $this->processedEntry('commands_inbox', '-10 days');

        $tester = $this->tester();
        self::assertSame(Command::FAILURE, $tester->execute(['transports' => ['commands_inbox', 'nope']]));
        self::assertStringContainsString('"nope" does not exist', $tester->getDisplay());

        $tester = $this->tester();
        self::assertSame(Command::FAILURE, $tester->execute(['transports' => ['commands']]));
        self::assertStringContainsString('"commands" is not an inbox transport', $tester->getDisplay());

        self::assertSame([$old], $this->indexedIds('commands_inbox'), 'Nothing is pruned when a transport is rejected');
    }

    private function processedEntry(string $inbox, string $processedAgo): string
    {
        $uuid = (string) Uuid::v7();
        $id = $this->connections[$inbox]->send('body', [], 0, $uuid);
        self::assertNotNull($id);
        self::assertTrue($this->connections[$inbox]->ack($id, $uuid));
        $this->dbal->executeStatement(
            \sprintf('UPDATE %s_index SET processed_at = ? WHERE id = ?', $inbox),
            [new \DateTimeImmutable($processedAgo, new \DateTimeZone('UTC')), $uuid],
            [Types::DATETIME_IMMUTABLE],
        );

        return $uuid;
    }

    /**
     * @return list<string>
     */
    private function indexedIds(string $inbox): array
    {
        $ids = array_map(
            static fn (mixed $id): string => \is_scalar($id) ? (string) $id : '',
            $this->dbal->fetchFirstColumn(\sprintf('SELECT id FROM %s_index', $inbox)),
        );
        sort($ids);

        return $ids;
    }
}
