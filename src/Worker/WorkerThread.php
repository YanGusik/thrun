<?php

declare(strict_types=1);

namespace Thrun\Worker;

use Async\OperationCanceledException;
use Async\ThreadChannel;
use Async\ThreadChannelException;
use Thrun\Envelope\Envelope;
use Thrun\Envelope\Stamp\TimeoutStamp;
use Thrun\Exception\TimeoutException;
use function Async\await;
use function Async\delay;
use function sprintf;

final class WorkerThread
{
    /**
     * @param array<class-string, callable(object, ?Acknowledger): void> $handlers
     */
    public function __construct(
        private readonly ThreadChannel $jobChannel,
        private readonly ThreadChannel $resultChannel,
        private readonly array         $handlers,
        private readonly int           $concurrency,
    ) {}

    public function run(): void
    {
        ini_set('memory_limit', '-1');

        $group = new \Async\TaskGroup($this->concurrency);

        try {
            while (true) {
                /** @var Envelope $envelope */
                $envelope = $this->jobChannel->recv();

                $group->spawn(function () use ($envelope): void {
                    $start = hrtime(true);

                    try {
                        $timeoutStamp = $envelope->last(TimeoutStamp::class);
                        $timeoutMs = $timeoutStamp instanceof TimeoutStamp ? $timeoutStamp->timeoutMs : 0;

                        if ($timeoutMs > 0) {
                            $ack = $this->runWithTimeout($envelope, $timeoutMs);
                        } else {
                            $ack = $this->runHandler($envelope);
                        }

                        $this->sendResult($envelope, $ack, (hrtime(true) - $start) / 1e9);
                    } catch (\Throwable $e) {
                        $this->resultChannel->send([
                            'ok' => false,
                            'envelope' => $envelope,
                            'timedOut' => false,
                            'error' => $e,
                            'processingTime' => (hrtime(true) - $start) / 1e9,
                            'wasRetried' => false,
                        ]);
                    }
                });
            }
        } catch (ThreadChannelException|OperationCanceledException) {
            // closed or cancelled
        } finally {
            $group->seal();
            try {
                $group->awaitCompletion();
            } catch (\Throwable) {
                // ignore
            }
        }
    }

    private function runWithTimeout(Envelope $envelope, int $timeoutMs): Acknowledger
    {
        $handlerScope = new \Async\Scope();
        $future = $handlerScope->spawn(function () use ($envelope): Acknowledger {
            return $this->runHandler($envelope);
        });

        try {
            return await($future, \Async\timeout($timeoutMs));
        } catch (OperationCanceledException $e) {
            $handlerScope->asNotSafely()->cancel();
            delay(50);
            $ack = new Acknowledger($envelope);
            $ack->fail(new TimeoutException($timeoutMs));
            $ack->markTimedOut();
            return $ack;
        } catch (\Throwable $e) {
            $handlerScope->asNotSafely()->cancel();
            throw $e;
        }
    }

    private function runHandler(Envelope $envelope): Acknowledger
    {
        $message = $envelope->message;
        $handler = $this->handlers[$message::class] ?? null;

        if ($handler === null) {
            throw new \RuntimeException(
                sprintf('No handler for "%s"', $message::class),
            );
        }

        $ack = new Acknowledger($envelope);

        $ref = new \ReflectionFunction($handler);
        if ($ref->getNumberOfParameters() >= 2) {
            $handler($message, $ack);
        } else {
            $handler($message);
            $ack->ack();
        }

        return $ack;
    }

    private function sendResult(Envelope $envelope, Acknowledger $ack, float $processingTime): void
    {
        if ($ack->isRetried()) {
            $this->resultChannel->send([
                'ok' => false,
                'envelope' => $envelope,
                'timedOut' => false,
                'error' => $ack->getFailureError() ?? new \RuntimeException('Retry requested by handler'),
                'processingTime' => $processingTime,
                'wasRetried' => true,
                'retryDelayMs' => $ack->getRetryDelayMs(),
            ]);
            return;
        }

        if ($ack->isFailed()) {
            $this->resultChannel->send([
                'ok' => false,
                'envelope' => $envelope,
                'timedOut' => $ack->isTimedOut(),
                'error' => $ack->getFailureError() ?? new \RuntimeException('Failed by handler'),
                'processingTime' => $processingTime,
                'wasRetried' => false,
            ]);
            return;
        }

        $this->resultChannel->send([
            'ok' => true,
            'envelope' => $envelope,
            'processingTime' => $processingTime,
        ]);
    }
}
