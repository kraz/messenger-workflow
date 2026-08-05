<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Fixture\Handler;

use Contracts\Mini\Event\MiniEvent;
use Doctrine\DBAL\Connection;
use Kraz\MessengerWorkflow\Application\Attribute\AsEventHandler;

/**
 * No-inbox event flow: consumed straight from the broker queue.
 * No fromTransport scoping — without an inbox the receiving transport is the broker
 * itself, so per-queue scoping is not available (documented reduction trade-off).
 */
#[AsEventHandler]
final class MiniEventHandler
{
    /**
     * @var list<string>
     */
    public static array $received = [];

    /**
     * @var list<bool>
     */
    public static array $inTransaction = [];

    public static function reset(): void
    {
        self::$received = [];
        self::$inTransaction = [];
    }

    public function __construct(private readonly Connection $postgres)
    {
    }

    public function __invoke(MiniEvent $event): void
    {
        self::$received[] = $event->payload;
        self::$inTransaction[] = $this->postgres->isTransactionActive();
    }
}
