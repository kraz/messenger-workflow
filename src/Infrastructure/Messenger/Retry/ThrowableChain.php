<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Infrastructure\Messenger\Retry;

use Symfony\Component\Messenger\Exception\HandlerFailedException;

final readonly class ThrowableChain
{
    /**
     * Yields the throwable itself, the exceptions wrapped by a HandlerFailedException
     * and every "previous" ancestor — the full causal chain a retry decider may want
     * to inspect.
     *
     * @return iterable<\Throwable>
     */
    public static function unwrap(\Throwable $throwable): iterable
    {
        $queue = [$throwable];
        $seen = [];

        while ([] !== $queue) {
            $current = array_shift($queue);
            $objectId = spl_object_id($current);
            if (isset($seen[$objectId])) {
                continue;
            }
            $seen[$objectId] = true;

            yield $current;

            if ($current instanceof HandlerFailedException) {
                foreach ($current->getWrappedExceptions() as $wrapped) {
                    $queue[] = $wrapped;
                }
            }
            if (null !== $current->getPrevious()) {
                $queue[] = $current->getPrevious();
            }
        }
    }
}
