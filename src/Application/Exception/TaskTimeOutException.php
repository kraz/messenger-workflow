<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Application\Exception;

use Symfony\Component\Messenger\Exception\TransportException;

class TaskTimeOutException extends TransportException
{
}
