<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Infrastructure\Messenger\Stamp;

/**
 * Marks a stamp that must survive when a message is re-enveloped while crossing
 * flow segments (outbox relay, broker receive, inbox handoff).
 */
interface TransferableStampInterface
{
}
