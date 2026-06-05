<?php

declare(strict_types=1);

namespace Thrun\Envelope\Stamp;

use Thrun\Contract\HeaderStampInterface;

final class MessageIdStamp implements HeaderStampInterface
{
    public function __construct(public readonly string|int $id) {}
}
