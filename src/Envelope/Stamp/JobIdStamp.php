<?php

declare(strict_types=1);

namespace Thrun\Envelope\Stamp;

use Ramsey\Uuid\Uuid;
use Thrun\Contract\StampInterface;

final class JobIdStamp implements StampInterface
{
    public readonly string $id;

    public function __construct(?string $id = null)
    {
        $this->id = $id ?? Uuid::uuid7()->toString();
    }
}
