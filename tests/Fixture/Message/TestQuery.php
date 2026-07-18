<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Fixture\Message;

use Kraz\MessengerWorkflow\Application\QueryInterface;

final class TestQuery implements QueryInterface
{
    public function __construct(
        public string $subject = 'subject',
    ) {
    }
}
