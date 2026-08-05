<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Infrastructure\Messenger\Stamp;

use Symfony\Component\Messenger\Envelope;

final readonly class TransferableStamps
{
    public static function extract(Envelope $envelope): Envelope
    {
        return self::to(new Envelope($envelope->getMessage()), $envelope);
    }

    public static function to(Envelope $target, Envelope $source): Envelope
    {
        foreach ($source->all() as $class => $stamps) {
            if (is_subclass_of($class, TransferableStampInterface::class)) {
                $target = $target->with(...$stamps);
            }
        }

        return $target;
    }
}
