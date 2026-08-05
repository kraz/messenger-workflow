<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Fixture;

use Kraz\MessengerWorkflow\Application\Task\TaskOwnerResolverInterface;

/**
 * Test resolver: the "current user" is whatever the test sets (null = no user
 * context, i.e. ownerless system tasks).
 */
final class FixedTaskOwnerResolver implements TaskOwnerResolverInterface
{
    public static ?string $owner = null;

    public function resolveOwnerIdentifier(): ?string
    {
        return self::$owner;
    }
}
