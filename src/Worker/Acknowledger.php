<?php

declare(strict_types=1);

namespace Thrun\Worker;

use Thrun\Envelope\Envelope;

final class Acknowledger
{
    private bool $acked = false;
    private bool $retried = false;
    private bool $failed = false;
    private bool $timedOut = false;
    private int $retryDelayMs = 0;
    private ?\Throwable $failureError = null;

    public function __construct(public readonly Envelope $envelope) {}

    public function ack(): void
    {
        $this->acked = true;
    }

    public function retry(int $delayMs = 0): void
    {
        $this->retried = true;
        $this->retryDelayMs = $delayMs;
    }

    public function fail(?\Throwable $error = null): void
    {
        $this->failed = true;
        $this->failureError = $error;
    }

    public function markTimedOut(): void
    {
        $this->timedOut = true;
    }

    public function isAcked(): bool
    {
        return $this->acked;
    }

    public function isRetried(): bool
    {
        return $this->retried;
    }

    public function isFailed(): bool
    {
        return $this->failed;
    }

    public function isTimedOut(): bool
    {
        return $this->timedOut;
    }

    public function getRetryDelayMs(): int
    {
        return $this->retryDelayMs;
    }

    public function getFailureError(): ?\Throwable
    {
        return $this->failureError;
    }
}
