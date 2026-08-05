<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Infrastructure\Messenger\Stamp;

use Symfony\Component\Messenger\Stamp\StampInterface;

/**
 * Marks a message that belongs to an ordered (FIFO) flow: transports must not
 * reorder it relative to other strict-order messages of the same queue.
 */
final readonly class StrictOrderStamp implements StampInterface, TransferableStampInterface
{
}
