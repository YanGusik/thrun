<?php

declare(strict_types=1);

namespace Thrun\Envelope\Stamp;

use Thrun\Contract\StampInterface;

final class ErrorDetailsStamp implements StampInterface
{
    public function __construct(
        public readonly string $exceptionClass,
        public readonly string $message,
        public readonly int $code,
        public readonly ?string $trace = null,
    ) {}

    public static function fromThrowable(\Throwable $e): self
    {
        return new self(
            exceptionClass: $e::class,
            message: $e->getMessage(),
            code: $e->getCode(),
            trace: $e->getTraceAsString(),
        );
    }
}
