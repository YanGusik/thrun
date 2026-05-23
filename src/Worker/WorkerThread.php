<?php

declare(strict_types=1);

namespace Thrun\Worker;

use Async\OperationCanceledException;
use Async\TaskSet;
use Async\ThreadChannel;
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
    /**
     * @param array<class-string, callable(object, ?Acknowledger): void> $handlers
     * @param array<int, WorkerMiddlewareInterface> $middleware
     */
    public function __construct(
        private readonly ThreadChannel $jobChannel,
        private readonly ThreadChannel $resultChannel,
        private readonly array         $handlers,
        private readonly int           $concurrency,
        private readonly array         $middleware = [],
    ) {}

    public function run(): void
    {
        ini_set('memory_limit', '-1');

        echo "count cc:" . $this->concurrency . "\n";
        $group = new TaskSet($this->concurrency);

        try {
            while (true) {
                /** @var Envelope $envelope */
                $envelope = $this->jobChannel->recv();
                if ($this->jobChannel->isClosed())
                {
                    $group->cancel();
                    throw new \Cancellation("JobChannel is closed");
                }

                $group->spawn(function () use ($envelope): void {
                    if ($this->jobChannel->isClosed())
                    {
                        return;
                    }

                    $start = hrtime(true);

                    try {
                        $timeoutStamp = $envelope->last(TimeoutStamp::class);
                        $timeoutMs = $timeoutStamp instanceof TimeoutStamp ? $timeoutStamp->timeoutMs : 0;

                        if ($timeoutMs > 0) {
                            $ack = $this->runWithTimeout($envelope, $timeoutMs);
                        } else {
                            $ack = $this->runHandler($envelope);
                        }
                        $tmp= $this->jobChannel->isClosed() ? 'true' : 'false';
                        echo "is_closed: $tmp end\n";

                        $this->sendResult($envelope, $ack, (hrtime(true) - $start) / 1e9);
                    } catch (\Throwable $e) {
                        echo "throwable in job".get_class($e) . ":" . $e->getMessage() . "\n";
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
        } catch (\Async\ThreadChannelException|OperationCanceledException|\Cancellation) {
            error_log('[Thrun WorkerThread] stopped.');
            // closed or cancelled
//            error_log('[Thrun WorkerThread] ThreadChannelException|OperationCanceledException');
        } catch (\Throwable $e) {
            error_log('[Thrun WorkerThread] Fatal error in thread: ' . $e::class . ': ' . $e->getMessage());
        } finally {
            try {
                $group->awaitCompletion();
            } catch (\Throwable) {
                error_log('[Thrun WorkerThread] catch (\Throwable)');
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
        $pipeline = $this->buildPipeline($handler);
        $pipeline($message, $ack);

        return $ack;
    }

    private function buildPipeline(callable $handler): \Closure
    {
        $next = function (object $message, Acknowledger $ack) use ($handler): void {
            $ref = $handler instanceof \Closure
                ? new \ReflectionFunction($handler)
                : new \ReflectionMethod($handler, '__invoke');
            if ($ref->getNumberOfParameters() >= 2) {
                $handler($message, $ack);
            } else {
                $handler($message);
                $ack->ack();
            }
        };

        foreach (array_reverse($this->middleware) as $middleware) {
            $next = function (object $message, Acknowledger $ack) use ($middleware, $next): void {
                $middleware->handle($message, $ack, $next);
            };
        }

        return $next;
    }

    private function sendResult(Envelope $envelope, Acknowledger $ack, float $processingTime): void
    {
        try {
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
        } catch (\Cancellation|\Async\ThreadChannelException $e) {

        } catch (\Throwable $e) {
            error_log('[Thrun WorkerThread] Failed to send result: ' . $e::class . ': ' . $e->getMessage());
        }
    }
}
