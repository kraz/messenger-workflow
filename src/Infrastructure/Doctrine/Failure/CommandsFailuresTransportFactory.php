<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Infrastructure\Doctrine\Failure;

use Doctrine\Persistence\ConnectionRegistry;
use Symfony\Component\Messenger\Bridge\Doctrine\Transport\DoctrineTransportFactory;
use Symfony\Component\Messenger\Exception\TransportException;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;
use Symfony\Component\Messenger\Transport\TransportInterface;

/**
 * Dead-letter storage for permanently failed commands — the stock Symfony Doctrine
 * transport under a workflow DSN scheme, DSN host = DBAL connection name.
 */
class CommandsFailuresTransportFactory extends DoctrineTransportFactory
{
    protected const string DSN_PREFIX = 'commands-failures://';
    protected const string DEFAULT_TABLE_NAME = 'zz_commands_failures';

    public function __construct(private readonly ?ConnectionRegistry $connectionRegistry = null)
    {
        if (null !== $connectionRegistry) {
            parent::__construct($connectionRegistry);
        }
    }

    /**
     * @param array<array-key, mixed> $options
     */
    public function createTransport(#[\SensitiveParameter] string $dsn, array $options, SerializerInterface $serializer): TransportInterface
    {
        if (null === $this->connectionRegistry) {
            throw new TransportException(\sprintf('The failure transport "%s" requires DoctrineBundle (the "doctrine" service is not available).', $dsn));
        }

        $options['table_name'] ??= static::DEFAULT_TABLE_NAME;

        return parent::createTransport($dsn, $options, $serializer);
    }

    /**
     * @param array<array-key, mixed> $options
     */
    public function supports(#[\SensitiveParameter] string $dsn, array $options): bool
    {
        return str_starts_with($dsn, static::DSN_PREFIX);
    }
}
