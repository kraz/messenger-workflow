<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Infrastructure\Doctrine\Failure;

/**
 * Dead-letter storage for permanently failed events.
 */
class EventsFailuresTransportFactory extends CommandsFailuresTransportFactory
{
    protected const string DSN_PREFIX = 'events-failures://';
    protected const string DEFAULT_TABLE_NAME = 'zz_events_failures';
}
