<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Application\Task;

/**
 * Supplies the identifier of the user owning tasks dispatched in the current
 * execution context (D7 — the package ships no security dependency; applications
 * typically implement this over their security token storage).
 */
interface TaskOwnerResolverInterface
{
    /**
     * @return string|null Null when no user context exists (worker processes, CLI) —
     *                     such tasks are ownerless system tasks and are not recorded
     */
    public function resolveOwnerIdentifier(): ?string;
}
