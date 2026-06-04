<?php

declare(strict_types=1);

namespace Thrun\Worker\Metrics;

use Thrun\Contract\MetricsInterface;

final class InMemoryMetrics implements MetricsInterface
{
    public int $processed = 0;
    public int $failed = 0;
    public int $retried = 0;
    public int $timedOut = 0;
    public int $active = 0;

    /** @var list<float> */
    public array $processingTimes = [];

    public function incrementProcessed(): void
    {
        $this->processed++;
    }

    public function incrementFailed(): void
    {
        $this->failed++;
    }

    public function incrementRetried(): void
    {
        $this->retried++;
    }

    public function incrementTimedOut(): void
    {
        $this->timedOut++;
    }

    public function recordProcessingTime(float $seconds): void
    {
        $this->processingTimes[] = $seconds;
    }

    public function incrementActive(): void
    {
        $this->active++;
    }

    public function decrementActive(): void
    {
        $this->active = max(0, $this->active - 1);
    }

    public function averageTime(): float
    {
        if ($this->processingTimes === []) {
            return 0.0;
        }

        return array_sum($this->processingTimes) / count($this->processingTimes);
    }

    public function throughput($startTime, $processed): float
    {
        $elapsed = (hrtime(true) - $startTime) / 1e9;

        return $elapsed > 0 ? ($this->processed - $processed) / $elapsed : 0.0;
    }
}
