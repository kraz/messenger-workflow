<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Messenger\Amqp;

use Jwage\PhpAmqpLibMessengerBundle\Transport\ConnectionFactory;
use Kraz\MessengerWorkflow\Messenger\Transport\ResultStorageInterface;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;
use Symfony\Component\Messenger\Transport\TransportFactoryInterface;
use Symfony\Component\Messenger\Transport\TransportInterface;

class AmqpTransportFactory implements TransportFactoryInterface
{
    public function __construct(
        private ConnectionFactory $connectionFactory,
        protected readonly ResultStorageInterface $resultStorage,
    ) {
    }

    public function createTransport(#[\SensitiveParameter] string $dsn, array $options, SerializerInterface $serializer): TransportInterface
    {
        unset($options['transport_name']);

        $connection = $this->connectionFactory->fromDsn($dsn, $options);

        return new AmqpTransport(
            connection: $connection,
            serializer: $serializer,
            resultStorage: $this->resultStorage,
        );
    }

    public function supports(#[\SensitiveParameter] string $dsn, array $options): bool
    {
        return str_starts_with($dsn, 'phpamqplib://') || str_starts_with($dsn, 'phpamqplibs://');
    }
}
