<?php

declare(strict_types=1);

namespace Thrun\Contract;

interface MetricsInterface
{
    public function incrementProcessed(): void;

    public function incrementFailed(): void;

    public function incrementRetried(): void;

    public function incrementTimedOut(): void;

    public function recordProcessingTime(float $seconds): void;
}
