<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Application\Task;

use Kraz\MessengerWorkflow\Application\Exception\TaskFailedException;
use Kraz\MessengerWorkflow\Application\Task\Exception\ResultStorageWaitTimeoutException;
use Symfony\Component\Messenger\Exception\TransportException;

/**
 * Stores task outcomes (results or failures) under the task/message id and lets
 * callers block until an outcome arrives.
 */
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
     * @param int $timeout Maximum waiting time in seconds
     *
     * @throws ResultStorageWaitTimeoutException when no result arrived within the timeout
     * @throws TaskFailedException               when the stored outcome is a failure
     * @throws TransportException
     */
    public function await(string $messageId, int $timeout): mixed;
}
