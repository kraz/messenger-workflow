<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Infrastructure\Messenger\Retry;

use Doctrine\DBAL\Exception\ConnectionException;
use Doctrine\DBAL\Exception\RetryableException;
use PhpAmqpLib\Exception\AMQPChannelClosedException;
use PhpAmqpLib\Exception\AMQPConnectionBlockedException;
use PhpAmqpLib\Exception\AMQPConnectionClosedException;
use PhpAmqpLib\Exception\AMQPIOException;
use PhpAmqpLib\Exception\AMQPTimeoutException;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\TransportException;

/**
 * Built-in last decider of the chain: infrastructure hiccups that a short
 * retry usually heals — database connection loss, deadlocks/serialization failures,
 * AMQP connection problems and messenger transport errors. Has no opinion on
 * anything else.
 */
final class TransientErrorRetryDecider implements RetryDeciderInterface
{
    private const array TRANSIENT_EXCEPTIONS = [
        ConnectionException::class,
        RetryableException::class,
        AMQPConnectionClosedException::class,
        AMQPConnectionBlockedException::class,
        AMQPChannelClosedException::class,
        AMQPIOException::class,
        AMQPTimeoutException::class,
        TransportException::class,
    ];

    public function decide(Envelope $envelope, \Throwable $throwable): ?bool
    {
        foreach (ThrowableChain::unwrap($throwable) as $exception) {
            foreach (self::TRANSIENT_EXCEPTIONS as $class) {
                if ($exception instanceof $class) {
                    return true;
                }
            }
        }

        return null;
    }
}
