<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Support;

use Symfony\Bundle\FrameworkBundle\Test\KernelTestCase;

abstract class WorkflowKernelTestCase extends KernelTestCase
{
    protected function tearDown(): void
    {
        parent::tearDown();

        // The kernel/framework may leave exception handlers registered which PHPUnit >= 11
        // reports as risky tests. Pop any leftover handlers.
        while (true) {
            $previousHandler = set_exception_handler(static fn () => null);
            restore_exception_handler();

            if (null === $previousHandler) {
                break;
            }

            restore_exception_handler();
        }
    }
}
