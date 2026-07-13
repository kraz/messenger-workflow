<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Application\Messenger;

interface HandlerTransactionInterface
{
    public function begin(): void;

    public function commit(): void;

    public function rollback(): void;

    public function wrap(callable $func): mixed;

    public function isSameTransactionProvider(mixed $provider): bool;
}
