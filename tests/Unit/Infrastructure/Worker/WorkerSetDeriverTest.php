<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Unit\Infrastructure\Worker;

use Kraz\MessengerWorkflow\Infrastructure\Worker\WorkerSetDeriver;
use PHPUnit\Framework\TestCase;

final class WorkerSetDeriverTest extends TestCase
{
    private WorkerSetDeriver $deriver;

    /**
     * The BookAppDemo1 BookStore module topology.
     *
     * @var array<string, string>
     */
    private const array BOOK_STORE_TRANSPORTS = [
        'book_store_commands' => 'commands-inbox://book_store',
        'book_store_commands_failures' => 'commands-failures://book_store?queue_name=book_store_events',
        'book_store_commands_notifier' => 'commands-outbox://book_store',
        'book_store_outbox' => 'events-outbox://book_store',
        'book_store_events' => 'events-inbox://book_store',
        'book_store_events_failures' => 'events-failures://book_store?queue_name=book_store_events',
    ];

    /**
     * @var array<string, array<string, array<string, string>>>
     */
    private const array BOOK_STORE_BINDINGS = [
        'events' => ['book_store_events' => ['owner' => 'BookStore']],
        'commands' => ['book_store_commands' => ['owner' => 'BookStore']],
        'queries' => ['book_store_queries' => ['owner' => 'BookStore']],
    ];

    protected function setUp(): void
    {
        $this->deriver = new WorkerSetDeriver();
    }

    /**
     * @param list<array<array-key, mixed>> $workers
     *
     * @return array<string, array<array-key, mixed>>
     */
    private function indexByName(array $workers): array
    {
        $indexed = [];
        foreach ($workers as $worker) {
            self::assertIsString($worker['name'] ?? null);
            $indexed[$worker['name']] = $worker;
        }

        return $indexed;
    }

    public function testTheFullFlowTopologyDerivesTheSevenClassicWorkers(): void
    {
        $workers = $this->indexByName($this->deriver->derive(self::BOOK_STORE_TRANSPORTS, self::BOOK_STORE_BINDINGS));

        self::assertCount(7, $workers);

        self::assertSame('event_publisher', $workers['book_store_outbox publisher']['type']);
        self::assertSame('relay.bus', $workers['book_store_outbox publisher']['target']);
        self::assertSame('book_store', $workers['book_store_outbox publisher']['group']);

        self::assertSame('command_notifier', $workers['book_store_commands_notifier']['type']);
        self::assertSame('notifier.bus', $workers['book_store_commands_notifier']['target']);
        self::assertSame('book_store_commands_notifier', $workers['book_store_commands_notifier']['source']);

        self::assertSame('command_receiver', $workers['book_store_commands receiver']['type']);
        self::assertSame('commands', $workers['book_store_commands receiver']['source']);
        self::assertSame('inbox.bus', $workers['book_store_commands receiver']['target']);
        self::assertSame('book_store_commands', $workers['book_store_commands receiver']['queue']);
        self::assertSame(['sleep' => 0.0], $workers['book_store_commands receiver']['cmd_extra_options']);

        self::assertSame('command_handler', $workers['book_store_commands handler']['type']);
        self::assertSame('book_store_commands', $workers['book_store_commands handler']['source']);
        self::assertSame('command.bus', $workers['book_store_commands handler']['target']);

        self::assertSame('event_receiver', $workers['book_store_events receiver']['type']);
        self::assertSame('event_handler', $workers['book_store_events handler']['type']);
        self::assertSame('event.bus', $workers['book_store_events handler']['target']);

        self::assertSame('query_handler', $workers['book_store_queries handler']['type']);
        self::assertSame('queries', $workers['book_store_queries handler']['source']);
        self::assertSame('book_store_queries', $workers['book_store_queries handler']['queue']);
        self::assertSame('query.bus', $workers['book_store_queries handler']['target']);
    }

    public function testWithoutAnInboxReceiverAndHandlerCollapseIntoOneWorker(): void
    {
        $workers = $this->indexByName($this->deriver->derive(
            [], // no inbox/outbox transports at all
            ['commands' => ['app_commands' => ['owner' => 'App']]],
        ));

        self::assertCount(1, $workers);
        $worker = $workers['app_commands handler'];
        self::assertSame('command_handler', $worker['type']);
        self::assertSame('commands', $worker['source'], 'Consumes the broker transport directly');
        self::assertSame('app_commands', $worker['queue']);
        self::assertSame('command.bus', $worker['target']);
        self::assertSame(['sleep' => 0.0], $worker['cmd_extra_options']);
    }

    public function testAFullyManualConfigurationProducesExactlyTheDeclaredSet(): void
    {
        // Old-style configs declare every worker; identity matching (type+source+queue)
        // replaces the derived twins so nothing is duplicated.
        $manual = [
            ['name' => 'Book store events publisher', 'group' => 'Book store', 'type' => 'event_publisher', 'source' => 'book_store_outbox', 'target' => 'relay.bus'],
            ['name' => 'Book store events receiver', 'group' => 'Book store', 'type' => 'event_receiver', 'source' => 'events', 'queue' => 'book_store_events', 'target' => 'inbox.bus'],
            ['name' => 'Book store events handler', 'group' => 'Book store', 'type' => 'event_handler', 'source' => 'book_store_events', 'target' => 'event.bus'],
            ['name' => 'Book store commands receiver', 'group' => 'Book store', 'type' => 'command_receiver', 'source' => 'commands', 'queue' => 'book_store_commands', 'target' => 'inbox.bus'],
            ['name' => 'Book store commands handler', 'group' => 'Book store', 'type' => 'command_handler', 'source' => 'book_store_commands', 'target' => 'command.bus'],
            ['name' => 'Book store commands notifier', 'group' => 'Book store', 'type' => 'command_notifier', 'source' => 'book_store_commands_notifier', 'target' => 'notifier.bus'],
            ['name' => 'Book store queries handler', 'group' => 'Book store', 'type' => 'query_handler', 'source' => 'queries', 'queue' => 'book_store_queries', 'target' => 'query.bus'],
        ];

        $workers = $this->deriver->derive(self::BOOK_STORE_TRANSPORTS, self::BOOK_STORE_BINDINGS, $manual);

        self::assertCount(7, $workers, 'Manual workers replace their derived twins — no duplicates');
        $names = array_column($workers, 'name');
        self::assertContains('Book store events publisher', $names);
        self::assertNotContains('book_store_outbox publisher', $names);
    }

    public function testAnOverrideMatchingByNameMergesIntoTheDerivedWorker(): void
    {
        $workers = $this->indexByName($this->deriver->derive(
            self::BOOK_STORE_TRANSPORTS,
            self::BOOK_STORE_BINDINGS,
            [['name' => 'book_store_commands handler', 'instances' => 4, 'labels' => ['tier' => 'hot']]],
        ));

        self::assertCount(7, $workers);
        $handler = $workers['book_store_commands handler'];
        self::assertSame(4, $handler['instances']);
        self::assertSame(['tier' => 'hot'], $handler['labels']);
        self::assertSame('command.bus', $handler['target'], 'Derived attributes survive the merge');
    }

    public function testEnabledFalseRemovesADerivedWorker(): void
    {
        $workers = $this->deriver->derive(
            self::BOOK_STORE_TRANSPORTS,
            self::BOOK_STORE_BINDINGS,
            [['name' => 'book_store_queries handler', 'enabled' => false]],
        );

        self::assertCount(6, $workers);
        self::assertNotContains('book_store_queries handler', array_column($workers, 'name'));
    }

    public function testUnmatchedManualWorkersAreAppended(): void
    {
        $workers = $this->deriver->derive(
            [],
            [],
            [['name' => 'Custom relay', 'group' => 'Ops', 'type' => 'event_publisher', 'source' => 'special_outbox', 'target' => 'relay.bus']],
        );

        self::assertCount(1, $workers);
        self::assertSame('Custom relay', $workers[0]['name']);
    }

    public function testARoutedQueueDerivesItsOwnReceiverAndHandlerInTheContextsGroup(): void
    {
        $workers = $this->indexByName($this->deriver->derive(
            self::BOOK_STORE_TRANSPORTS + ['book_store_planning' => 'commands-inbox://book_store?strict_order=true'],
            [
                'commands' => [
                    'book_store_commands' => ['owner' => 'BookStore'],
                    'book_store_planning' => ['owner' => 'BookStore', 'route' => 'planning', 'messages' => ['X']],
                ],
            ],
        ));

        self::assertCount(6, $workers, 'Publisher, notifier, 2 × (receiver + handler) — no notifier worker for the routed queue');
        self::assertSame('command_receiver', $workers['book_store_planning receiver']['type']);
        self::assertSame('book_store_planning', $workers['book_store_planning receiver']['queue']);
        self::assertSame('command_handler', $workers['book_store_planning handler']['type']);
        self::assertSame('book_store_planning', $workers['book_store_planning handler']['source']);
        self::assertSame('book_store', $workers['book_store_planning handler']['group'], 'The routed workers join the group of the owner\'s regular queue');
        self::assertSame('book_store', $workers['book_store_planning receiver']['group']);
        self::assertArrayNotHasKey('instances', $workers['book_store_planning handler'], 'instances defaults to 1');
    }

    public function testARoutedQueryQueueJoinsTheContextsGroup(): void
    {
        $workers = $this->indexByName($this->deriver->derive([], [
            'queries' => [
                'book_store_queries' => ['owner' => 'BookStore'],
                'book_store_reports' => ['owner' => 'BookStore', 'route' => 'reports', 'messages' => ['X']],
            ],
        ]));

        self::assertSame('book_store', $workers['book_store_reports handler']['group']);
        self::assertSame('book_store_reports', $workers['book_store_reports handler']['queue']);
    }
}
