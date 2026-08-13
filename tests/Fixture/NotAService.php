<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Fixture;

/**
 * Deliberately never registered in any test container — nullable handler-method
 * parameters of this type must degrade to null.
 */
final class NotAService
{
}
