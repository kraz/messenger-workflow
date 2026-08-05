<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Fixture\Handler;

use Contracts\Nano\Command\NanoCommand;
use Doctrine\DBAL\Connection;
use Kraz\MessengerWorkflow\Application\Attribute\AsCommandHandler;

/**
 * Minimal flow: no outbox, no inbox, no notifier, no orm mapping —
 * the handler runs without any transaction and the tracked result is written to the
 * result storage directly from the handler worker.
 */
#[AsCommandHandler]
final class NanoCommandHandler
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

    public function __invoke(NanoCommand $command): string
    {
        self::$inTransaction[] = $this->postgres->isTransactionActive();

        return 'nano:'.$command->payload;
    }
}
