<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Messenger\Middleware;

use Doctrine\Persistence\ManagerRegistry;
use Kraz\MessengerWorkflow\Messenger\Stamp\AsyncMessageStamp;
use Kraz\MessengerWorkflow\Messenger\Stamp\AwaitMessageResultStamp;
use Kraz\MessengerWorkflow\Messenger\Transport\ResultStorageInterface;
use Psr\Container\ContainerInterface;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Handler\HandlersLocatorInterface;
use Symfony\Component\Messenger\Middleware\MiddlewareInterface;
use Symfony\Component\Messenger\Middleware\StackInterface;
use Symfony\Component\Messenger\Stamp\ReceivedStamp;

class QueryMiddleware implements MiddlewareInterface
{
    use HandleTargetEventTrait;

    public function __construct(
        protected ContainerInterface $receiverLocator,
        protected HandlersLocatorInterface $handlersLocator,
        protected ManagerRegistry $managerRegistry,
        protected ResultStorageInterface $resultStorage,
        protected array $queueOrmBinding,
    ) {
    }

    public function handle(Envelope $envelope, StackInterface $stack): Envelope
    {
        if ($envelope->all(ReceivedStamp::class)) {
            return $this->handleTargetEvent($envelope, $stack, $this->receiverLocator, $this->managerRegistry, $this->queueOrmBinding);
        } elseif (!$envelope->last(AsyncMessageStamp::class)) {
            /** @var \Generator $handlers */
            $handlers = $this->handlersLocator->getHandlers($envelope);
            $handler = $handlers->current();
            $canHandleLocally = null !== $handler;
            if ($canHandleLocally) {
                $envelope = $envelope->with(new ReceivedStamp('internal'));
            } else {
                $awaitResultStamp = $envelope->last(AwaitMessageResultStamp::class);
                if (null !== $awaitResultStamp && $awaitResultStamp->isDeferred()) {
                    // The transport will handle the await of message result
                    return $stack->next()->handle($envelope, $stack);
                }

                // Remote queries via gRPC — to be implemented in a future version (when PHP/FrankenPHP has native support for gRPC server)
                throw new \RuntimeException('The query message can not be executed synchronously!');
            }
        }

        return $stack->next()->handle($envelope, $stack);
    }
}
