<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Integration\Infrastructure;

use Kraz\MessengerWorkflow\Tests\Support\Infra;
use PHPUnit\Framework\Attributes\Group;
use PHPUnit\Framework\TestCase;

#[Group('rabbitmq')]
final class RabbitMqSmokeTest extends TestCase
{
    public function testConnectAndDeclareTransientQueue(): void
    {
        $connection = Infra::requireAmqp();

        try {
            $channel = $connection->channel();

            /** @var array{0: string, 1: int, 2: int}|null $declared */
            $declared = $channel->queue_declare('', durable: false, exclusive: true, auto_delete: true);
            self::assertNotNull($declared);
            self::assertNotSame('', $declared[0]);

            $channel->close();
        } finally {
            $connection->close();
        }
    }
}
