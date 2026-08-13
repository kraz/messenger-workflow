<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Infrastructure\Messenger\Handler;

/**
 * Invokes a handler method that declares extra parameters after the message,
 * passing the container-resolved arguments positionally after it.
 *
 * Registered by MethodHandlerArgumentsPass as the messenger handler wrapping
 * the original service method.
 */
final class HandlerMethodInvoker
{
    private readonly \Closure $handler;

    /**
     * @param list<mixed> $arguments
     */
    public function __construct(
        object $handler,
        string $method,
        private readonly array $arguments,
    ) {
        $callable = [$handler, $method];
        if (!\is_callable($callable)) {
            throw new \InvalidArgumentException(\sprintf('Handler method "%s::%s()" is not callable.', $handler::class, $method));
        }

        $this->handler = \Closure::fromCallable($callable);
    }

    public function __invoke(object $message): mixed
    {
        return ($this->handler)($message, ...$this->arguments);
    }
}
