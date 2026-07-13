<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Application\Messenger;

use Kraz\MessengerWorkflow\Application\Exception\TaskFailedException;
use Kraz\MessengerWorkflow\Application\Exception\TaskTimeOutException;

interface CommandBusInterface
{
    /**
     * Start synchronous command.
     *
     * @param object   $command Command request
     * @param int|null $timeout Maximum waiting time (in seconds) after which a TaskTimeOutException is thrown (null - use default/configured value)
     *
     * @throws TaskTimeOutException
     * @throws TaskFailedException
     */
    public function dispatch(object $command, ?int $timeout = null): void;

    /**
     * Start asynchronous command task.
     *
     * @param object $command Command request
     *
     * @return string Task ID
     */
    public function dispatchAsync(object $command): string;

    /**
     * Wait for asynchronous command Task to complete.
     *
     * @param string   $taskId  Task ID returned from "dispatchAsync" method
     * @param int|null $timeout Maximum waiting time (in seconds) after which a TaskTimeOutException is thrown (null - use default/configured value)
     *
     * @throws TaskTimeOutException
     * @throws TaskFailedException
     */
    public function await(string $taskId, ?int $timeout = null): void;
}
