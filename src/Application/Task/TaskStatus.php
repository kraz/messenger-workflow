<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Application\Task;

/**
 * The observable lifecycle status of an asynchronous command/query task. The
 * messaging engine exposes no intermediate statuses: a task is only ever pending,
 * completed or failed.
 */
enum TaskStatus: string
{
    case Pending = 'pending';
    case Completed = 'completed';
    case Failed = 'failed';
}
