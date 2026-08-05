<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Application\Messenger;

use Kraz\MessengerWorkflow\Application\Exception\PendingOutboxMessageException;
use Kraz\MessengerWorkflow\Application\Exception\TaskFailedException;
use Kraz\MessengerWorkflow\Application\Exception\TaskTimeOutException;

interface CommandBusInterface
{
    /**
     * Start an asynchronous command task.
     *
     * The command always travels through the message broker (or the configured
     * transactional outbox) — it is never executed synchronously in the
     * dispatching process.
     *
     * Passing the $taskId argument opts in to result tracking: the variable
     * receives the Task ID (set before the method returns), the command result
     * (or failure) is written to the result storage, and the Task ID can be used
     * with await() or the task status/result providers. When the argument is
     * omitted the command is fire-and-forget and no result storage is involved
     * anywhere in the flow.
     *
     * @param object      $command Command request
     * @param string|null $taskId  Receives the Task ID by reference (pass a variable to enable result tracking)
     */
    public function dispatch(object $command, ?string &$taskId = null): void;

    /**
     * Wait for an asynchronous command task to complete.
     *
     * Returns normally when the task completed successfully. The task result
     * value (if any) can be read through the task result provider services.
     *
     * @param string   $taskId  Task ID returned by reference from the "dispatch" method
     * @param int|null $timeout Maximum waiting time (in seconds) after which a TaskTimeOutException is thrown (null - use default/configured value)
     *
     * @throws TaskTimeOutException           when the task did not complete within the timeout
     * @throws TaskFailedException            when the task failed (data taken from the result storage)
     * @throws PendingOutboxMessageException  when the dispatched message is still held in a transactional
     *                                        outbox by an active (uncommitted) database transaction of this
     *                                        process and therefore can never complete before commit
     */
    public function await(string $taskId, ?int $timeout = null): void;
}
