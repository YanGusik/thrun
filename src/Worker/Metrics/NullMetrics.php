<?php

declare(strict_types=1);

namespace Thrun\Worker\Metrics;

use Thrun\Contract\MetricsInterface;

final class NullMetrics implements MetricsInterface
{
    public function incrementProcessed(): void {}

    public function incrementFailed(): void {}

    public function incrementRetried(): void {}

    public function incrementTimedOut(): void {}

    public function recordProcessingTime(float $seconds): void {}
}
