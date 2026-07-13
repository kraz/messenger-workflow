<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Application\Messenger;

use Kraz\MessengerWorkflow\Application\Exception\TaskFailedException;
use Kraz\MessengerWorkflow\Application\Exception\TaskTimeOutException;

interface QueryBusInterface
{
    /**
     * Start synchronous query.
     *
     * @param object   $query   Query request
     * @param int|null $timeout Maximum waiting time (in seconds) after which a TaskTimeOutException is thrown (null - use default/configured value)
     *
     * @throws TaskTimeOutException
     * @throws TaskFailedException
     *
     * @return mixed Query result
     */
    public function ask(object $query, ?int $timeout = null): mixed;

    /**
     * Start asynchronous query task.
     *
     * @param object $query Query request
     *
     * @return string Task ID
     */
    public function askAsync(object $query): string;

    /**
     * Wait for asynchronous query Task to complete.
     *
     * @param string   $taskId  Task ID returned from "askAsync" method
     * @param int|null $timeout Maximum waiting time (in seconds) after which a TaskTimeOutException is thrown (null - use default/configured value)
     *
     * @throws TaskTimeOutException
     * @throws TaskFailedException
     *
     * @return mixed Query result
     */
    public function await(string $taskId, ?int $timeout = null): mixed;
}
