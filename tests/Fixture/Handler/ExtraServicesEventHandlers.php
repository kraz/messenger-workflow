<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Fixture\Handler;

use Kraz\MessengerWorkflow\Application\Attribute\AsEventHandler;
use Kraz\MessengerWorkflow\Tests\Fixture\Message\ExtraServicesEvent;
use Kraz\MessengerWorkflow\Tests\Fixture\MessageRecorder;

/**
 * Two classes handling the same event through methods with extra service arguments —
 * their wrapped handlers must keep distinct descriptor names, or the second one is
 * skipped as "already handled" within a single dispatch.
 */
final class ExtraServicesEventHandlers
{
    #[AsEventHandler]
    public function onExtraServicesEvent(ExtraServicesEvent $event, MessageRecorder $recorder): void
    {
        $recorder->record(self::class.'::onExtraServicesEvent', $event);
    }
}
