<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Messenger\Stamp;

use Symfony\Component\Messenger\Envelope;

final readonly class TransferableStamps
{
    public static function extract(Envelope $envelope): Envelope
    {
        $result = new Envelope($envelope->getMessage());

        return self::to($result, $envelope);
    }

    public static function to(Envelope $target, Envelope $source): Envelope
    {
        $stamps = $source->all();
        foreach ($stamps as $class => $items) {
            if (is_subclass_of($class, TransferableStampInterface::class)) {
                $target = $target->with(...$items);
            }
        }

        return $target;
    }
}
