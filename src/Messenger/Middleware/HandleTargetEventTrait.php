<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Messenger\Middleware;

use Doctrine\DBAL\Exception;
use Doctrine\ORM\EntityManagerInterface;
use Doctrine\Persistence\ManagerRegistry;
use Kraz\MessengerWorkflow\Application\CommandInterface;
use Kraz\MessengerWorkflow\Application\QueryInterface;
use Kraz\MessengerWorkflow\Domain\DomainEventInterface;
use Kraz\MessengerWorkflow\Messenger\Doctrine\Handler\DoctrineHandlerTransaction;
use Kraz\MessengerWorkflow\Messenger\Doctrine\Transport\Outbox\OutboxTransport;
use Kraz\MessengerWorkflow\Messenger\Event\CommandCompletedNotification;
use Kraz\MessengerWorkflow\Messenger\Stamp\MessageIdStamp;
use Kraz\MessengerWorkflow\Messenger\Stamp\ResultStorageStamp;
use Kraz\MessengerWorkflow\Messenger\Stamp\TargetTransportNameStamp;
use Kraz\MessengerWorkflow\Messenger\Transport\Handler\TransportHandlerTransaction;
use Kraz\MessengerWorkflow\Messenger\Transport\TransactionalTransportInterface;
use Psr\Container\ContainerExceptionInterface;
use Psr\Container\ContainerInterface;
use Psr\Container\NotFoundExceptionInterface;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\UnrecoverableMessageHandlingException;
use Symfony\Component\Messenger\Middleware\StackInterface;
use Symfony\Component\Messenger\Stamp\HandledStamp;
use Symfony\Component\Messenger\Stamp\HandlerArgumentsStamp;
use Symfony\Component\Messenger\Stamp\ReceivedStamp;
use Symfony\Component\Messenger\Stamp\TransportNamesStamp;
use Webmozart\Assert\Assert;

trait HandleTargetEventTrait
{
    /**
     * @throws \Throwable
     * @throws NotFoundExceptionInterface
     * @throws ContainerExceptionInterface
     * @throws Exception
     */
    private function handleTargetEvent(Envelope $envelope, StackInterface $stack, ContainerInterface $receiverLocator, ManagerRegistry $managerRegistry, array $queueOrmBinding): Envelope
    {
        $targetTransportName = $envelope->last(TargetTransportNameStamp::class)->getTransportName();
        if (!$targetTransportName) {
            throw new UnrecoverableMessageHandlingException('Can not handle the message. The target messenger transport can not be determined!');
        }

        $transportNames = $envelope->last(TransportNamesStamp::class)?->getTransportNames() ?? [];
        if (\count($transportNames) > 0 && !\in_array($targetTransportName, $transportNames, true)) {
            throw new UnrecoverableMessageHandlingException(\sprintf('Can not handle the event. The expected transport name is "%s", but the currently being processed are: %s', $targetTransportName, implode(', ', $transportNames)));
        }

        $transport = null;
        $entityManager = null;
        $handlerTransaction = null;

        if ($receiverLocator->has($targetTransportName)) {
            $expectedTransportName = $envelope->last(ReceivedStamp::class)?->getTransportName();
            if ($expectedTransportName !== $targetTransportName) {
                throw new UnrecoverableMessageHandlingException(\sprintf('Can not handle the message. The messenger transport was expected to be "%s", but the current is "%s".', $expectedTransportName, $targetTransportName));
            }
            $transport = $receiverLocator->get($targetTransportName);
            if ($transport instanceof TransactionalTransportInterface) {
                if ($transport->isTransactionActive()) {
                    $targetTransportName = $envelope->last(TargetTransportNameStamp::class)->getTransportName();
                    throw new UnrecoverableMessageHandlingException(\sprintf('Can not handle the message. The messenger transport "%s" is in active transaction!', $targetTransportName));
                }
                $handlerTransaction = new TransportHandlerTransaction($transport, $envelope);
            }
        } else {
            $entityManagerName = $queueOrmBinding[$targetTransportName] ?? null;
            if ($entityManagerName) {
                /** @var EntityManagerInterface $entityManager */
                $entityManager = $managerRegistry->getManager($entityManagerName);
                if ($entityManager->getConnection()->isTransactionActive()) {
                    $targetTransportName = $envelope->last(TargetTransportNameStamp::class)->getTransportName();
                    throw new UnrecoverableMessageHandlingException(\sprintf('Can not handle the message. The messenger ORM transport "%s" is in active transaction!', $targetTransportName));
                }
                $handlerTransaction = new DoctrineHandlerTransaction($entityManager);
            }
        }

        $message = $envelope->getMessage();

        if ($message instanceof DomainEventInterface) {
            $transportName = $envelope->last(ReceivedStamp::class)?->getTransportName();
            if ('events' === $transportName) {
                $envelope = $envelope
                    ->withoutAll(ReceivedStamp::class)
                    ->with(new ReceivedStamp($targetTransportName));
            } elseif (!$transport && !$entityManager) {
                throw new UnrecoverableMessageHandlingException('Can not determine the underlying transport or ORM for the event.');
            }
            if ($handlerTransaction) {
                return $this->handleResult($stack->next()->handle($envelope->with(new HandlerArgumentsStamp([$handlerTransaction])), $stack));
            }

            return $this->handleResult($stack->next()->handle($envelope, $stack));
        }

        if ($message instanceof CommandInterface) {
            if (!$handlerTransaction) {
                throw new UnrecoverableMessageHandlingException('Can not determine the underlying transport or ORM for the command transaction.');
            }

            $useResultStorage = null !== $envelope->last(ResultStorageStamp::class);
            if ($useResultStorage) {
                $notifierTransportName = $targetTransportName.'_notifier';
                if (!$receiverLocator->has($notifierTransportName)) {
                    throw new UnrecoverableMessageHandlingException(\sprintf('Can not handle the message. The notifier messenger transport "%s" for handler messenger transport "%s" was not found.', $targetTransportName, $notifierTransportName));
                }
                $notifierTransport = $receiverLocator->get($notifierTransportName);
                if (!$notifierTransport instanceof OutboxTransport) {
                    throw new UnrecoverableMessageHandlingException(\sprintf('Can not handle the message. The notifier messenger transport must be an instance of "%s"', OutboxTransport::class));
                }
                if (!$handlerTransaction->isSameTransactionProvider($notifierTransport)) {
                    throw new UnrecoverableMessageHandlingException(\sprintf('Can not handle the message. The transaction provider for the notifier messenger transport ("%s") must be the same with the one for the messenger target transport (%s).', $notifierTransportName, $targetTransportName));
                }

                return $handlerTransaction->wrap(function () use ($stack, $envelope, $notifierTransport, $notifierTransportName) {
                    $envelope = $stack->next()->handle($envelope, $stack);
                    if ($handledStamp = $envelope->last(HandledStamp::class)) {
                        $messageId = $envelope->last(MessageIdStamp::class)?->getMessageId();
                        Assert::stringNotEmpty($messageId);
                        $result = $handledStamp->getResult();
                        $notifierTransport->send(new Envelope(new CommandCompletedNotification($messageId, $result), [
                            new TransportNamesStamp([$notifierTransportName]),
                        ]));
                    }

                    return $envelope;
                });
            } else {
                return $this->handleResult($handlerTransaction->wrap(fn () => $stack->next()->handle($envelope, $stack)));
            }
        }

        if ($message instanceof QueryInterface) {
            if ($handlerTransaction) {
                return $this->handleResult($stack->next()->handle($envelope->with(new HandlerArgumentsStamp([$handlerTransaction])), $stack));
            }

            return $this->handleResult($stack->next()->handle($envelope, $stack));
        }

        return $stack->next()->handle($envelope, $stack);
    }

    private function handleResult(Envelope $envelope): Envelope
    {
        $message = $envelope->getMessage();
        if ($message instanceof DomainEventInterface) {
            return $envelope;
        }

        $useResultStorage = null !== $envelope->last(ResultStorageStamp::class);

        if ($useResultStorage && $handledStamp = $envelope->last(HandledStamp::class)) {
            $messageId = $envelope->last(MessageIdStamp::class)?->getMessageId();
            Assert::stringNotEmpty($messageId);
            $this->resultStorage->write($messageId, $handledStamp->getResult());
        }

        return $envelope;
    }
}
