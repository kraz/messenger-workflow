<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Infrastructure\Doctrine;

use Doctrine\DBAL\Connection as DbalConnection;

/**
 * Resolves the database (DBAL connection) owning a workflow transport — required to
 * process a message and its retranslation atomically in multi-database setups (one
 * database per bounded context). Sources: the inbox and outbox transports registered
 * in this process.
 */
final class TransportDatabaseResolver
{
    public function __construct(
        private readonly WorkflowTransportRegistry $transportRegistry,
    ) {
    }

    public function resolveConnection(string $transportName): ?DbalConnection
    {
        $transport = $this->transportRegistry->getInboxTransport($transportName)
            ?? $this->transportRegistry->getOutboxTransport($transportName);

        return $transport?->getConnection()->getDriverConnection();
    }
}
