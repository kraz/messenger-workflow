<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Messenger;

use Kraz\MessengerWorkflow\Application\Messenger\QueryBusInterface;
use Kraz\MessengerWorkflow\Application\QueryInterface;
use Kraz\MessengerWorkflow\Messenger\Stamp\AsyncMessageStamp;
use Kraz\MessengerWorkflow\Messenger\Stamp\AwaitMessageResultStamp;
use Kraz\MessengerWorkflow\Messenger\Stamp\MessageIdStamp;
use Kraz\MessengerWorkflow\Messenger\Stamp\QueryStamp;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\HandlerFailedException;
use Symfony\Component\Messenger\Exception\LogicException;
use Symfony\Component\Messenger\MessageBusInterface;
use Symfony\Component\Messenger\Stamp\HandledStamp;
use Symfony\Component\Uid\Uuid;

class QueryBus implements QueryBusInterface
{
    private MessageBusInterface $messageBus;
    private int $awaitDefaultTimeout;

    public function __construct(MessageBusInterface $queryBus, int $awaitDefaultTimeout = 300)
    {
        $this->messageBus = $queryBus;
        $this->awaitDefaultTimeout = $awaitDefaultTimeout;
    }

    /**
     * @throws \Throwable
     */
    public function ask(object $query, ?int $timeout = null): mixed
    {
        $envelope = Envelope::wrap($query);
        $envelope = $envelope
            ->withoutAll(AsyncMessageStamp::class)
            ->withoutAll(AwaitMessageResultStamp::class)
            ->with(
                new AwaitMessageResultStamp($timeout ?? $this->awaitDefaultTimeout),
            );
        try {
            return $this->handle($envelope);
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
    public function askAsync(object $query): string
    {
        $envelope = Envelope::wrap($query);
        $envelope = $envelope
            ->withoutAll(AsyncMessageStamp::class)
            ->with(
                new AsyncMessageStamp(),
            );
        try {
            $envelope = $this->push($envelope);
            $messageIdStamp = $envelope->last(MessageIdStamp::class);
            if (!$messageIdStamp) {
                throw new LogicException('Can not find the message ID of the query request!');
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
    public function await(string $taskId, ?int $timeout = null): mixed
    {
        $envelope = Envelope::wrap(new class implements QueryInterface {});
        $envelope = $envelope
            ->withoutAll(MessageIdStamp::class)
            ->withoutAll(AwaitMessageResultStamp::class)
            ->with(
                new MessageIdStamp($taskId),
                new AwaitMessageResultStamp($timeout ?? $this->awaitDefaultTimeout, true),
            );
        try {
            return $this->handle($envelope);
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
        $queryStamp = $envelope->last(QueryStamp::class);
        if (!$queryStamp) {
            $messageId = $envelope->last(MessageIdStamp::class)?->getMessageId() ?: (string) Uuid::v7();
            $routingKey = (string) RoutingKey::createForDirectTransport($envelope->getMessage(), 'queries');
            $queryStamp = new QueryStamp(
                messageId: $messageId,
                routingKey: $routingKey,
            );
            $envelope = $envelope->with($queryStamp);
        }

        if (!$envelope->getMessage() instanceof QueryInterface) {
            throw new \RuntimeException(\sprintf('Invalid query message. Expected an instance of "%s", but got %s', QueryInterface::class, \get_class($envelope->getMessage())));
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
