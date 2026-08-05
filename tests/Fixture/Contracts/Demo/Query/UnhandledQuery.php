<?php

declare(strict_types=1);

namespace Contracts\Demo\Query;

use Kraz\MessengerWorkflow\Application\QueryInterface;

/**
 * A contract query no deployed context handles — used to verify the consume-time
 * exactly-one-handler enforcement (D10) for queries.
 */
final class UnhandledQuery implements QueryInterface
{
    public function __construct(public readonly string $payload = 'test')
    {
    }
}
