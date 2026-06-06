<?php

declare(strict_types=1);

namespace Thrun\Transport\Failed;

use Thrun\Contract\FailedJobStoreInterface;
use Thrun\Envelope\Envelope;

final class NullFailedJobSender implements FailedJobStoreInterface
{
    public function send(Envelope $envelope): void
    {
        // No-op
    }

    public function flush(): void
    {
        // No-op
    }
}
