<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Messenger\Middleware;

use Doctrine\Persistence\ManagerRegistry;
use Kraz\MessengerWorkflow\Application\Messenger\EventBusInterface;
use Kraz\MessengerWorkflow\Messenger\Stamp\SourceTransportNameStamp;
use Kraz\MessengerWorkflow\Messenger\Stamp\StrictOrderStamp;
use Kraz\MessengerWorkflow\Messenger\Stamp\TargetTransportNameStamp;
use Kraz\MessengerWorkflow\Messenger\Stamp\TransferableStamps;
use Kraz\MessengerWorkflow\Messenger\Transport\ResultStorageInterface;
use Psr\Container\ContainerInterface;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\RecoverableMessageHandlingException;
use Symfony\Component\Messenger\Exception\UnrecoverableMessageHandlingException;
use Symfony\Component\Messenger\Middleware\MiddlewareInterface;
use Symfony\Component\Messenger\Middleware\StackInterface;
use Symfony\Component\Messenger\Stamp\HandledStamp;
use Symfony\Component\Messenger\Stamp\ReceivedStamp;

class EventMiddleware implements MiddlewareInterface
{
    use HandleTargetEventTrait;

    public function __construct(
        protected EventBusInterface $eventBus,
        protected ContainerInterface $receiverLocator,
        protected ManagerRegistry $managerRegistry,
        protected ResultStorageInterface $resultStorage,
        protected array $queueOrmBinding,
    ) {
    }

    public function handle(Envelope $envelope, StackInterface $stack): Envelope
    {
        if ($envelope->all(ReceivedStamp::class)) {
            if ($envelope->all(TargetTransportNameStamp::class)) {
                return $this->handleTargetEvent($envelope, $stack, $this->receiverLocator, $this->managerRegistry, $this->queueOrmBinding);
            }
            if ($envelope->all(SourceTransportNameStamp::class)) {
                try {
                    $eventEnvelope = TransferableStamps::extract($envelope);
                    $this->eventBus->publish($eventEnvelope);
                } catch (\Exception $exception) {
                    if ($envelope->all(StrictOrderStamp::class)) {
                        throw new UnrecoverableMessageHandlingException($exception->getMessage(), $exception->getCode(), $exception);
                    }
                    throw new RecoverableMessageHandlingException($exception->getMessage(), $exception->getCode(), $exception);
                }

                return $envelope->with(new HandledStamp(null, 'events'));
            }
        }

        return $stack->next()->handle($envelope, $stack);
    }
}
