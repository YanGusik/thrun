<?php

declare(strict_types=1);

namespace Thrun\Exception;

final class TimeoutException extends \RuntimeException
{
    public function __construct(int $timeoutMs)
    {
        parent::__construct(
            sprintf('Job timed out after %d ms', $timeoutMs),
        );
    }
}
