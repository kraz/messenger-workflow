<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Fixture\Handler;

use Contracts\Demo\Event\GammaEvent;
use Doctrine\DBAL\Connection;
use Kraz\MessengerWorkflow\Application\Attribute\AsEventHandler;

/**
 * Gamma context: events inbox with transactional_handler=true — the handler must run
 * inside the transaction opened on the inbox's database connection.
 */
#[AsEventHandler(fromTransport: 'gamma_events')]
final class GammaTransactionalEventHandler
{
    /**
     * @var list<bool>
     */
    public static array $inTransaction = [];

    public static function reset(): void
    {
        self::$inTransaction = [];
    }

    public function __construct(private readonly Connection $postgres)
    {
    }

    public function __invoke(GammaEvent $event): void
    {
        self::$inTransaction[] = $this->postgres->isTransactionActive();
    }
}
