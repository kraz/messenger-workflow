<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Messenger;

use Kraz\MessengerWorkflow\Application\CommandInterface;
use Kraz\MessengerWorkflow\Application\Messenger\CommandBusInterface;
use Kraz\MessengerWorkflow\Messenger\Stamp\AsyncMessageStamp;
use Kraz\MessengerWorkflow\Messenger\Stamp\AwaitMessageResultStamp;
use Kraz\MessengerWorkflow\Messenger\Stamp\CommandStamp;
use Kraz\MessengerWorkflow\Messenger\Stamp\MessageIdStamp;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\HandlerFailedException;
use Symfony\Component\Messenger\Exception\LogicException;
use Symfony\Component\Messenger\MessageBusInterface;
use Symfony\Component\Messenger\Stamp\HandledStamp;
use Symfony\Component\Uid\Uuid;

class CommandBus implements CommandBusInterface
{
    private MessageBusInterface $messageBus;
    private int $awaitDefaultTimeout;

    public function __construct(MessageBusInterface $commandBus, int $awaitDefaultTimeout = 300)
    {
        $this->messageBus = $commandBus;
        $this->awaitDefaultTimeout = $awaitDefaultTimeout;
    }

    /**
     * @throws \Throwable
     */
    public function dispatch(object $command, ?int $timeout = null): void
    {
        $envelope = Envelope::wrap($command);
        $envelope = $envelope
            ->withoutAll(AsyncMessageStamp::class)
            ->withoutAll(AwaitMessageResultStamp::class)
            ->with(
                new AwaitMessageResultStamp($timeout ?? $this->awaitDefaultTimeout),
            );
        try {
            $this->handle($envelope);
        } catch (HandlerFailedException $e) {
            /** @var array{0: \Throwable} $exceptions */
            $exceptions = $e->getWrappedExceptions();
            $exception = array_shift($exceptions);
            throw $exception ?? $e;
        }
    }

    /**
     * @throws \Throwable
     */
    public function dispatchAsync(object $command): string
    {
        $envelope = Envelope::wrap($command);
        $envelope = $envelope
            ->withoutAll(AsyncMessageStamp::class)
            ->with(
                new AsyncMessageStamp(),
            );
        try {
            $envelope = $this->push($envelope);
            $messageIdStamp = $envelope->last(MessageIdStamp::class);
            if (!$messageIdStamp) {
                throw new LogicException('Can find the message ID of the command request!');
            }

            return $messageIdStamp->getMessageId();
        } catch (HandlerFailedException $e) {
            /** @var array{0: \Throwable} $exceptions */
            $exceptions = $e->getWrappedExceptions();
            $exception = array_shift($exceptions);
            throw $exception ?? $e;
        }
    }

    /**
     * @throws \Throwable
     */
    public function await(string $taskId, ?int $timeout = null): void
    {
        $envelope = Envelope::wrap(new class implements CommandInterface {});
        $envelope = $envelope
            ->withoutAll(MessageIdStamp::class)
            ->withoutAll(AwaitMessageResultStamp::class)
            ->with(
                new MessageIdStamp($taskId),
                new AwaitMessageResultStamp($timeout ?? $this->awaitDefaultTimeout, true),
            );
        try {
            $this->handle($envelope);
        } catch (HandlerFailedException $e) {
            /** @var array{0: \Throwable} $exceptions */
            $exceptions = $e->getWrappedExceptions();
            $exception = array_shift($exceptions);
            throw $exception ?? $e;
        }
    }

    private function push(object $message): Envelope
    {
        $envelope = Envelope::wrap($message);

        $commandStamp = $envelope->last(CommandStamp::class);
        if (!$commandStamp) {
            $messageId = $envelope->last(MessageIdStamp::class)?->getMessageId() ?: (string) Uuid::v7();
            $routingKey = (string) RoutingKey::createForDirectTransport($envelope->getMessage(), 'commands');
            $commandStamp = new CommandStamp(
                messageId: $messageId,
                routingKey: $routingKey
            );
            $envelope = $envelope->with($commandStamp);
        }

        if (!$envelope->getMessage() instanceof CommandInterface) {
            throw new \RuntimeException(\sprintf('Invalid command message. Expected an instance of "%s", but got %s', CommandInterface::class, \get_class($envelope->getMessage())));
        }

        return $this->messageBus->dispatch($envelope);
    }

    private function handle(object $message): mixed
    {
        $envelope = $this->push($message);

        /** @var HandledStamp[] $handledStamps */
        $handledStamps = $envelope->all(HandledStamp::class);

        if (!$handledStamps) {
            throw new LogicException(\sprintf('Message of type "%s" was handled zero times. Exactly one handler is expected when using "%s::%s()".', get_debug_type($envelope->getMessage()), static::class, __FUNCTION__));
        }

        if (\count($handledStamps) > 1) {
            $handlers = implode(', ', array_map(function (HandledStamp $stamp): string {
                return \sprintf('"%s"', $stamp->getHandlerName());
            }, $handledStamps));

            throw new LogicException(\sprintf('Message of type "%s" was handled multiple times. Only one handler is expected when using "%s::%s()", got %d: %s.', get_debug_type($envelope->getMessage()), static::class, __FUNCTION__, \count($handledStamps), $handlers));
        }

        return $handledStamps[0]->getResult();
    }
}
