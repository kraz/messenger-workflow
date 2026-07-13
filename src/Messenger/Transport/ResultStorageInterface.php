<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Messenger\Transport;

use Kraz\MessengerWorkflow\Messenger\Transport\Exception\ResultStorageWaitTimeoutException;
use Symfony\Component\Messenger\Exception\TransportException;

interface ResultStorageInterface
{
    /**
     * @throws TransportException
     */
    public function write(string $messageId, mixed $data): void;

    /**
     * @throws TransportException
     */
    public function writeError(string $messageId, string $message, int|string|null $code = null, ?string $class = null, ?string $trace = null): void;

    /**
     * @throws ResultStorageWaitTimeoutException
     * @throws TransportException
     */
    public function await(string $messageId, int $timeout): mixed;
}
