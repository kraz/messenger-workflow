<?php

declare(strict_types=1);

namespace Contracts\Demo\Query;

use Kraz\MessengerWorkflow\Application\QueryInterface;

/**
 * A contract query with (deliberately) two registered handlers — the exactly-one
 * enforcement must fail it at consume time.
 */
final class AmbiguousQuery implements QueryInterface
{
    public function __construct(public readonly string $payload = 'test')
    {
    }
}
