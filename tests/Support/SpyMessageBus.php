<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Support;

use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\MessageBusInterface;

/**
 * Message bus double that records dispatched envelopes and lets the test
 * shape the returned envelope (e.g. add HandledStamp) or throw.
 */
final class SpyMessageBus implements MessageBusInterface
{
    /** @var list<Envelope> */
    public array $envelopes = [];

    public function __construct(
        private ?\Closure $onDispatch = null,
    ) {
    }

    public function dispatch(object $message, array $stamps = []): Envelope
    {
        $envelope = Envelope::wrap($message, $stamps);
        $this->envelopes[] = $envelope;

        return null !== $this->onDispatch ? ($this->onDispatch)($envelope) : $envelope;
    }

    public function lastEnvelope(): Envelope
    {
        if ([] === $this->envelopes) {
            throw new \LogicException('No envelope was dispatched.');
        }

        return $this->envelopes[array_key_last($this->envelopes)];
    }
}
