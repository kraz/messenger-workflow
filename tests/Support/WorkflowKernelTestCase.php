<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Support;

use Symfony\Bundle\FrameworkBundle\Test\KernelTestCase;

abstract class WorkflowKernelTestCase extends KernelTestCase
{
    protected function tearDown(): void
    {
        parent::tearDown();

        // FrameworkBundle registers an exception handler on kernel boot and never removes it,
        // which PHPUnit >= 11 reports as risky. Pop any handlers left behind.
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
