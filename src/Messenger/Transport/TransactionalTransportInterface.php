<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Messenger\Transport;

use Symfony\Component\Messenger\Transport\TransportInterface;

interface TransactionalTransportInterface extends TransportInterface
{
    public function beginTransaction(): void;

    public function commitTransaction(): void;

    public function rollbackTransaction(): void;

    public function isTransactionActive(): bool;

    public function isSameTransactionProvider(mixed $provider): bool;
}
