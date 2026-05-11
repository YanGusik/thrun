<?php

declare(strict_types=1);

namespace Thrun\Tests\Fixture;

final class SendEmailMessage
{
    public function __construct(
        public readonly string $to,
        public readonly string $subject,
    ) {}
}
