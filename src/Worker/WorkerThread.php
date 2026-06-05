<?php

declare(strict_types=1);

namespace Thrun\Worker;

use Async\Future;
use Async\FutureState;
use Async\OperationCanceledException;
use Async\Scope;
use Async\Signal;
use Async\TaskSet;
use Async\ThreadChannel;
use Thrun\Envelope\Envelope;
use Thrun\Envelope\Stamp\TimeoutStamp;
use Thrun\Exception\TimeoutException;
use function Async\await;
use function Async\await_any_or_fail;
use function Async\delay;
use function Async\signal;
use function Async\spawn;
use function sprintf;

final class WorkerThread
{
    /**
     * @param  array<string, callable(object|array, ?Acknowledger): void>  $handlers
     */
    /**
     * @param  array<string, callable(object|array, ?Acknowledger): void>  $handlers
     * @param  array<int, WorkerMiddlewareInterface>  $middleware
     */
    public function __construct(
        private readonly Envelope $envelope,
        private readonly ThreadChannel $resultChannel,
        private readonly array $handlers,
        private readonly array $middleware = [],
    ) {
    }

    public function run(): void
    {
        ini_set('memory_limit', '-1');
        $this->runTask($this->envelope);
    }

    private function runTask(Envelope $envelope): void
    {
        $start = hrtime(true);

        try {
            $timeoutStamp = $envelope->last(TimeoutStamp::class);
            $timeoutMs    = $timeoutStamp instanceof TimeoutStamp ? $timeoutStamp->timeoutMs : 0;

            if ($timeoutMs > 0) {
                $ack = $this->runWithTimeout($envelope, $timeoutMs);
            } else {
                $ack = $this->runHandler($envelope);
            }
//            echo "job processed!\n";

            $this->sendResult($envelope, $ack, (hrtime(true) - $start) / 1e9);
        } catch (\Throwable $e) {
            $this->resultChannel->send([
                'ok'             => false,
                'envelope'       => $envelope,
                'timedOut'       => false,
                'error'          => $e,
                'processingTime' => (hrtime(true) - $start) / 1e9,
                'wasRetried'     => false,
            ]);
        }
    }

    private function runWithTimeout(Envelope $envelope, int $timeoutMs): Acknowledger
    {
        $handlerScope = new \Async\Scope();
        $future       = $handlerScope->spawn(function () use ($envelope): Acknowledger {
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
        $key     = $envelope->routeKey ?? $envelope->type ?? gettype($message);
        $handler = $this->handlers[$key] ?? null;

        if ($handler === null) {
            throw new \RuntimeException(sprintf('No handler for "%s"', $key));
        }

        if (is_string($handler) && class_exists($handler)) {
            $handler = new $handler();
        }

        $ack      = new Acknowledger($envelope);
        $pipeline = $this->buildPipeline($handler);
        $pipeline($message, $ack);

        return $ack;
    }

    private function buildPipeline(callable $handler): \Closure
    {
        $next = function (object|array $message, Acknowledger $ack) use ($handler): void {
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
                    'ok'             => false,
                    'envelope'       => $envelope,
                    'timedOut'       => false,
                    'error'          => $ack->getFailureError() ?? new \RuntimeException('Retry requested by handler'),
                    'processingTime' => $processingTime,
                    'wasRetried'     => true,
                    'retryDelayMs'   => $ack->getRetryDelayMs(),
                ]);

                return;
            }

            if ($ack->isFailed()) {
                $this->resultChannel->send([
                    'ok'             => false,
                    'envelope'       => $envelope,
                    'timedOut'       => $ack->isTimedOut(),
                    'error'          => $ack->getFailureError() ?? new \RuntimeException('Failed by handler'),
                    'processingTime' => $processingTime,
                    'wasRetried'     => false,
                ]);

                return;
            }

            $this->resultChannel->send([
                'ok'             => true,
                'envelope'       => $envelope,
                'processingTime' => $processingTime,
            ]);
        } catch (\Cancellation|\Async\ThreadChannelException $e) {

        } catch (\Throwable $e) {
            error_log('[Thrun WorkerThread] Failed to send result: '.$e::class.': '.$e->getMessage());
        }
    }
}
