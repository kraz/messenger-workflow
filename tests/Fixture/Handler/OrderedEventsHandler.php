<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Fixture\Handler;

use Contracts\Demo\Event\OrderedEvent;
use Doctrine\DBAL\Connection;
use Kraz\MessengerWorkflow\Application\Attribute\AsEventHandler;

/**
 * Ordered-flow E2E handler (app_events, single-consumer FIFO inbox, non-transactional
 * by default). Every invocation — including failing ones — is appended to the static
 * log so tests can assert the exact processing order; payloads starting with "poison"
 * always fail.
 */
#[AsEventHandler(fromTransport: 'app_events')]
final class OrderedEventsHandler
{
    /**
     * @var list<string>
     */
    public static array $invocations = [];

    /**
     * @var list<bool>
     */
    public static array $inTransaction = [];

    public static function reset(): void
    {
        self::$invocations = [];
        self::$inTransaction = [];
    }

    public function __construct(private readonly Connection $postgres)
    {
    }

    public function __invoke(OrderedEvent $event): void
    {
        self::$invocations[] = $event->payload;
        self::$inTransaction[] = $this->postgres->isTransactionActive();

        if (str_starts_with($event->payload, 'poison')) {
            throw new \RuntimeException('poisoned event: '.$event->payload);
        }
    }
}
