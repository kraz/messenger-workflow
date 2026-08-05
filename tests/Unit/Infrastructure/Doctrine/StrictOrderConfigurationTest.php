<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Unit\Infrastructure\Doctrine;

use Doctrine\DBAL\Connection as DbalConnection;
use Doctrine\DBAL\DriverManager;
use Doctrine\Persistence\ConnectionRegistry;
use Kraz\MessengerWorkflow\Infrastructure\Doctrine\Connection;
use Kraz\MessengerWorkflow\Infrastructure\Doctrine\Inbox\CommandsInboxTransportFactory;
use Kraz\MessengerWorkflow\Infrastructure\Doctrine\Inbox\InboxTransport;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Messenger\Exception\InvalidArgumentException;
use Symfony\Component\Messenger\Transport\Serialization\PhpSerializer;

/**
 * Spec: strict_order requests single-consumer FIFO delivery and
 * is mutually exclusive with multiple_consumers (competing consumers reorder by design).
 */
final class StrictOrderConfigurationTest extends TestCase
{
    private function dbal(): DbalConnection
    {
        return DriverManager::getConnection(['driver' => 'pdo_sqlite', 'memory' => true]);
    }

    private function registry(DbalConnection $connection): ConnectionRegistry
    {
        return new class($connection) implements ConnectionRegistry {
            public function __construct(private readonly DbalConnection $connection)
            {
            }

            public function getDefaultConnectionName(): string
            {
                return 'default';
            }

            public function getConnection(?string $name = null): object
            {
                return $this->connection;
            }

            /**
             * @return array<string, object>
             */
            public function getConnections(): array
            {
                return ['default' => $this->connection];
            }

            /**
             * @return array<string, string>
             */
            public function getConnectionNames(): array
            {
                return ['default' => 'doctrine.dbal.default_connection'];
            }
        };
    }

    public function testStrictOrderWithMultipleConsumersIsRejected(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessageMatches('/mutually exclusive/');

        new Connection(['strict_order' => true, 'multiple_consumers' => true], $this->dbal());
    }

    public function testStrictOrderAloneIsAccepted(): void
    {
        $connection = new Connection(['strict_order' => true], $this->dbal());

        self::assertTrue($connection->isStrictOrder());
    }

    public function testBuildConfigurationNormalizesTheDsnStringToBool(): void
    {
        $configuration = Connection::buildConfiguration('inbox://default?strict_order=true');

        self::assertTrue($configuration['strict_order']);
    }

    public function testStrictOrderSuppressesTheCommandsInboxCompetingConsumersDefault(): void
    {
        $dbal = $this->dbal();
        $factory = new CommandsInboxTransportFactory($this->registry($dbal));

        $transport = $factory->createTransport('commands-inbox://default?strict_order=true', [], new PhpSerializer());

        self::assertInstanceOf(InboxTransport::class, $transport);
        $configuration = $transport->getConnection()->getConfiguration();
        self::assertFalse($configuration['multiple_consumers'], 'strict_order downgrades the commands inbox to a single FIFO consumer');
        self::assertTrue($configuration['strict_order']);
    }

    public function testStrictOrderWithAnExplicitMultipleConsumersOptionIsRejectedByTheFactory(): void
    {
        $dbal = $this->dbal();
        $factory = new CommandsInboxTransportFactory($this->registry($dbal));

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessageMatches('/mutually exclusive/');

        $factory->createTransport('commands-inbox://default?strict_order=true&multiple_consumers=true', [], new PhpSerializer());
    }

    public function testTheCommandsInboxDefaultRemainsCompetingConsumers(): void
    {
        $dbal = $this->dbal();
        $factory = new CommandsInboxTransportFactory($this->registry($dbal));

        $transport = $factory->createTransport('commands-inbox://default', [], new PhpSerializer());

        self::assertInstanceOf(InboxTransport::class, $transport);
        $configuration = $transport->getConnection()->getConfiguration();
        self::assertTrue($configuration['multiple_consumers']);
        self::assertFalse($configuration['strict_order']);
    }
}
