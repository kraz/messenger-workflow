<?php

declare(strict_types=1);

namespace Contracts\Demo\Query;

use Kraz\MessengerWorkflow\Application\QueryInterface;

final class GetSomethingQuery implements QueryInterface
{
    public function __construct(
        public string $what = 'it',
    ) {
    }
}
