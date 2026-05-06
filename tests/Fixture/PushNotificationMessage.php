<?php

declare(strict_types=1);

namespace Thrun\Tests\Fixture;

final class PushNotificationMessage
{
    public function __construct(public readonly int $userId) {}
}
