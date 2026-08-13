<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Fixture\Handler;

use Kraz\MessengerWorkflow\Application\Attribute\AsEventHandler;
use Kraz\MessengerWorkflow\Tests\Fixture\Message\ExtraServicesEvent;
use Kraz\MessengerWorkflow\Tests\Fixture\MessageRecorder;

/**
 * Second class handling ExtraServicesEvent — see ExtraServicesEventHandlers.
 */
final class MoreExtraServicesEventHandlers
{
    #[AsEventHandler]
    public function onExtraServicesEvent(ExtraServicesEvent $event, MessageRecorder $recorder): void
    {
        $recorder->record(self::class.'::onExtraServicesEvent', $event);
    }
}
