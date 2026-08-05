<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Fixture\Handler;

use Contracts\Demo\Event\SomethingHappened;
use Kraz\MessengerWorkflow\Application\Attribute\AsEventHandler;

/**
 * Second consumer context (beta — sqlite database): subscribes to the shared contract
 * event through its own queue/inbox. The beta_events binding key is auto-derived from
 * this handler's fromTransport attribute.
 */
#[AsEventHandler(fromTransport: 'beta_events')]
final class BetaEventHandler
{
    /**
     * @var list<string>
     */
    public static array $received = [];

    public static function reset(): void
    {
        self::$received = [];
    }

    public function __invoke(SomethingHappened $event): void
    {
        self::$received[] = $event->payload;
    }
}
