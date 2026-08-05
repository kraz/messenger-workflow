<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Fixture\Retry;

/**
 * Domain-looking failure that only the fixture ForcedRetryDecider knows to retry.
 */
final class ForceRetryOnceException extends \RuntimeException
{
}
