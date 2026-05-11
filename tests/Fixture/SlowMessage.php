<?php

declare(strict_types=1);

namespace Thrun\Tests\Fixture;

final class SlowMessage
{
    public function __construct(public readonly int $sleepMs) {}
}
