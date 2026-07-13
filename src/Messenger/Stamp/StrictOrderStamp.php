<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Messenger\Stamp;

use Symfony\Component\Messenger\Stamp\StampInterface;

class StrictOrderStamp implements StampInterface, TransferableStampInterface
{
}
