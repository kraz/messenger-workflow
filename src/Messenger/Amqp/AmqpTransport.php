<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Messenger\Amqp;

use Jwage\PhpAmqpLibMessengerBundle\Transport\AmqpReceiver as SymfonyAmqpReceiver;
use Jwage\PhpAmqpLibMessengerBundle\Transport\AmqpSender as SymfonyAmqpSender;
use Jwage\PhpAmqpLibMessengerBundle\Transport\AmqpStamp;
use Jwage\PhpAmqpLibMessengerBundle\Transport\AmqpTransport as SymfonyAmqpTransport;
use Jwage\PhpAmqpLibMessengerBundle\Transport\Connection;
use Kraz\MessengerWorkflow\Application\Exception\TaskTimeOutException;
use Kraz\MessengerWorkflow\Messenger\Stamp\AsyncMessageStamp;
use Kraz\MessengerWorkflow\Messenger\Stamp\AwaitMessageResultStamp;
use Kraz\MessengerWorkflow\Messenger\Stamp\CommandStamp;
use Kraz\MessengerWorkflow\Messenger\Stamp\EventStamp;
use Kraz\MessengerWorkflow\Messenger\Stamp\QueryStamp;
use Kraz\MessengerWorkflow\Messenger\Stamp\ResultStorageStamp;
use Kraz\MessengerWorkflow\Messenger\Transport\Exception\ResultStorageWaitTimeoutException;
use Kraz\MessengerWorkflow\Messenger\Transport\ResultStorageInterface;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Stamp\HandledStamp;
use Symfony\Component\Messenger\Transport\Serialization\PhpSerializer;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;
use Webmozart\Assert\Assert;

class AmqpTransport extends SymfonyAmqpTransport
{
    public function __construct(
        private Connection $connection,
        private ?SymfonyAmqpReceiver $amqpReceiver = null,
        private ?SymfonyAmqpSender $amqpSender = null,
        private ?SerializerInterface $serializer = null,
        private ?ResultStorageInterface $resultStorage = null,
    ) {
        parent::__construct($connection, $amqpReceiver, $amqpSender, $serializer);
    }

    #[\Override]
    public function get(?int $fetchSize = null): iterable
    {
        return $this->getReceiver()->get($fetchSize);
    }

    #[\Override]
    public function getFromQueues(array $queueNames, ?int $fetchSize = null): iterable
    {
        return $this->getReceiver()->getFromQueues($queueNames, $fetchSize);
    }

    #[\Override]
    public function ack(Envelope $envelope): void
    {
        $this->getReceiver()->ack($envelope);
    }

    #[\Override]
    public function reject(Envelope $envelope): void
    {
        $this->getReceiver()->reject($envelope);
    }

    #[\Override]
    public function send(Envelope $envelope): Envelope
    {
        $messageId = '';

        if ($eventStamp = $envelope->last(EventStamp::class)) {
            $amqpAttributes = [
                'message_id' => $messageId = $eventStamp->getMessageId(),
            ];

            $envelope = $envelope
                ->withoutAll(EventStamp::class)
                ->with(new AmqpStamp($eventStamp->getRoutingKey(), $amqpAttributes));
        } elseif ($commandStamp = $envelope->last(CommandStamp::class)) {
            $amqpAttributes = [
                'message_id' => $messageId = $commandStamp->getMessageId(),
            ];
            if (null !== $commandStamp->getPriority()) {
                $amqpAttributes['priority'] = $commandStamp->getPriority();
            }

            $envelope = $envelope
                ->withoutAll(CommandStamp::class)
                ->with(new AmqpStamp($commandStamp->getRoutingKey(), $amqpAttributes));
        } elseif ($queryStamp = $envelope->last(QueryStamp::class)) {
            $amqpAttributes = [
                'message_id' => $messageId = $queryStamp->getMessageId(),
            ];

            if (null !== $queryStamp->getPriority()) {
                $amqpAttributes['priority'] = $queryStamp->getPriority();
            }

            $envelope = $envelope
                ->withoutAll(QueryStamp::class)
                ->with(new AmqpStamp($queryStamp->getRoutingKey(), $amqpAttributes));
        }

        $isAsync = null !== $envelope->last(AsyncMessageStamp::class);
        $awaitResultStamp = $envelope->last(AwaitMessageResultStamp::class);
        Assert::nullOrIsInstanceOf($awaitResultStamp, AwaitMessageResultStamp::class);
        $awaitResult = $messageId && $awaitResultStamp && $this->resultStorage;

        if ($isAsync || $awaitResult) {
            $envelope = $envelope->withoutAll(ResultStorageStamp::class)->with(new ResultStorageStamp());
        }

        if (!$awaitResultStamp || !$awaitResultStamp->isDeferred()) {
            $envelope = parent::send($envelope);
        }

        if ($isAsync) {
            Assert::stringNotEmpty($messageId);

            return $envelope->with(new HandledStamp($messageId, 'async'));
        }

        if ($awaitResult) {
            try {
                $response = $this->resultStorage->await($messageId, $awaitResultStamp->getTimeout());

                return $envelope->with(new HandledStamp($response, 'async'));
            } catch (ResultStorageWaitTimeoutException) {
                throw new TaskTimeOutException('Timeout reached while waiting for the task result.');
            }
        }

        return $envelope;
    }

    #[\Override]
    public function flush(): void
    {
        $this->getSender()->flush();
    }

    #[\Override]
    public function getMessageCount(): int
    {
        return $this->getReceiver()->getMessageCount();
    }

    private function getSerializer(): SerializerInterface
    {
        return $this->serializer ??= new PhpSerializer();
    }

    private function getReceiver(): SymfonyAmqpReceiver
    {
        return $this->amqpReceiver ??= new AmqpReceiver($this->connection, $this->getSerializer());
    }

    private function getSender(): SymfonyAmqpSender
    {
        return $this->amqpSender ??= new AmqpSender($this->connection, $this->getSerializer());
    }
}
