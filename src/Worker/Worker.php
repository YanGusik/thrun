<?php

declare(strict_types=1);

namespace Thrun\Worker;


use Async\Scope;
use Async\ThreadChannel;
use Async\ThreadChannelException;
use Closure;
use Thrun\Contract\MetricsInterface;
use Thrun\Contract\ReceiverInterface;
use Thrun\Contract\SenderInterface;
use Thrun\Envelope\Envelope;
use Thrun\Envelope\Stamp\DelayStamp;
use Thrun\Envelope\Stamp\ErrorDetailsStamp;
use Thrun\Envelope\Stamp\RetryStamp;
use Thrun\Worker\Metrics\NullMetrics;
use Thrun\Worker\Retry\NoRetryStrategy;
use function Async\await;
use function Async\delay;
use function Async\protect;
use function Async\spawn;
use function Async\spawn_thread;

final class Worker
{
    private bool $running = false;
    private ?Scope $scope = null;
    private ?ThreadChannel $jobChannel = null;
    private ?ThreadChannel $resultChannel = null;

    /**
     * @param  array<class-string, callable(object, ?Acknowledger): void>  $handlers
     */
    public function __construct(
        private readonly ReceiverInterface $transport,
        private readonly array $handlers,
        private readonly WorkerOptions $options = new WorkerOptions(),
        private readonly ?SenderInterface $failureTransport = null,
        private readonly MetricsInterface $metrics = new NullMetrics(),
    ) {
    }

    public function run(): void
    {
        $this->running = true;

        // Larger capacity to prevent trivial deadlocks
        $capacity            = max(128, $this->options->threads * $this->options->concurrency * 2);
        $this->jobChannel    = new ThreadChannel($capacity);
        $this->resultChannel = new ThreadChannel($capacity);

        $threads = [];
        $this->fillThreads($threads);

        $this->scope = new Scope();

        $abnormalError = null;

        // health check: if all threads die unexpectedly, crash the worker so supervisor restarts it
        $healthCheckCoro = $this->scope->spawn($this->healthCheckCoro($threads, $abnormalError));

        // result reader coroutine
        $resultReaderCoro = $this->scope->spawn($this->resultReaderCoro());

        // producer loop coroutine
        $producer = $this->scope->spawn($this->producerCoro());

        try {
            await($producer);
        } catch (\Cancellation|\Async\ThreadChannelException) {
            error_log('[Thrun Worker] Producer \Cancellation or ThreadChannelException');
        } catch (\Throwable $e) {
            error_log('[Thrun Worker] Producer coroutine error: '.$e::class.': '.$e->getMessage());
            if ($abnormalError === null) {
                $abnormalError = new \RuntimeException('Worker stopped unexpectedly: '.$e->getMessage(), 0, $e);
            }
        } finally {
            $this->running = false;
        }

        try {
            await($resultReaderCoro);
        } catch (\Cancellation|\Async\ThreadChannelException) {
            error_log('[Thrun Worker] Result reader \Cancellation or ThreadChannelException');
        } catch (\Throwable $e) {
            error_log('[Thrun Worker] Result reader coroutine error: '.$e::class.': '.$e->getMessage());
            if ($abnormalError === null) {
                $abnormalError = new \RuntimeException('Worker stopped unexpectedly: '.$e->getMessage(), 0, $e);
            }
        } finally {
            $this->running = false;
        }

        try {
            await($healthCheckCoro);
        } catch (\Cancellation|\Async\ThreadChannelException) {
        } catch (\Throwable $e) {
            error_log('[Thrun Worker] Health-check coroutine error: '.$e::class.': '.$e->getMessage());
            if ($abnormalError === null) {
                $abnormalError = $e;
            }
        } finally {
            $this->running = false;
        }

        if ($abnormalError !== null) {
            throw $abnormalError;
        }
    }

    public function stop(): void
    {
        error_log('[Thrun Worker] Stopped');
        $this->running = false;
        $this->scope?->cancel();
        $this->transport->close();
        $this->jobChannel?->close();
        $this->resultChannel?->close();
    }

    private function fillThreads(array &$threads): void
    {
        $handlers    = $this->handlers;
        $concurrency = $this->options->concurrency;
        $middleware  = $this->options->middleware;
        $bootloader  = $this->options->bootloader ?? $this->detectBootloader();

        $jobChannel    = $this->jobChannel;
        $resultChannel = $this->resultChannel;

        for ($i = 0; $i < $this->options->threads; $i++) {
            $threads[] = spawn_thread(
                static function () use ($handlers, $concurrency, $middleware, $jobChannel, $resultChannel): void {
                    (new WorkerThread($jobChannel, $resultChannel, $handlers, $concurrency, $middleware))->run();
                },
                bootloader: $bootloader,
            );
        }
    }

    private function producerCoro(): Closure
    {
        return function (): void {
            while ($this->running) {
                $envelope = $this->transport->receive();
                if ($envelope === null) {
                    break;
                }
                $this->jobChannel->send($envelope);

                $this->metrics->incrementActive();

                if ($this->options->onDispatch !== null) {
                    ($this->options->onDispatch)($envelope);
                }
            }
        };
    }

    private function resultReaderCoro(): Closure
    {
        return function (): void {
            while (true) {
                /** @var array{ok: bool, envelope: Envelope, timedOut?: bool, error?: \Throwable|null, processingTime?: float, wasRetried?: bool, retryDelayMs?: int|null} $result */
                $result = $this->resultChannel->recv();

                $this->metrics->decrementActive();

                if ($this->options->onResult !== null) {
                    ($this->options->onResult)($result);
                }

                $this->metrics->recordProcessingTime($result['processingTime'] ?? 0);

                if ($result['ok']) {
                    $this->metrics->incrementProcessed();
                    $this->transport->ack($result['envelope']);
                } else {
                    $this->metrics->incrementFailed();

                    if ($result['timedOut'] ?? false) {
                        $this->metrics->incrementTimedOut();
                    }

                    $wasRetried = $this->handleFailure(
                        $result['envelope'],
                        $result['error'] ?? null,
                        $result['retryDelayMs'] ?? null,
                    );

                    if ($wasRetried) {
                        $this->metrics->incrementRetried();
                    }
                }
            }
        };
    }

    private function healthCheckCoro($threads, &$abnormalError): Closure
    {
        return function () use ($threads, &$abnormalError): void {
            while ($this->running) {
                delay(1000);
                if (!$this->running) {
                    return;
                }

                $allDead = true;
                foreach ($threads as $thread) {
                    if (!$thread->isCompleted() && !$thread->isCancelled()) {
                        $allDead = false;
                        break;
                    }
                }
                if ($allDead) {
                    $abnormalError = new \RuntimeException('All worker threads have died');
                    $this->jobChannel->close();
                    $this->resultChannel->close();
                    $this->stop();

                    return;
                }
            }
        };
    }

    private function handleFailure(Envelope $envelope, ?\Throwable $error, ?int $explicitRetryDelayMs): bool
    {
        $retryStamp = $envelope->last(RetryStamp::class);
        $strategy   = $retryStamp?->strategy ?? new NoRetryStrategy();
        $attempt    = $retryStamp?->attempts ?? 0;

        // Explicit retry from handler takes priority over strategy
        if ($explicitRetryDelayMs !== null) {
            $this->transport->reject($envelope);
            $this->transport->send($envelope->with(
                new RetryStamp(attempts: $attempt + 1, strategy: $strategy),
                new DelayStamp(delayMs: $explicitRetryDelayMs),
            ));

            return true;
        }

        if ($strategy->isRetryable($error ?? new \RuntimeException('Unknown error'), $attempt + 1)) {
            $this->transport->reject($envelope);
            $this->transport->send($envelope->with(
                new RetryStamp(attempts: $attempt + 1, strategy: $strategy),
                new DelayStamp(delayMs: $strategy->getDelay($attempt + 1)),
            ));

            return true;
        }

        $this->transport->reject($envelope);
        if ($this->failureTransport !== null && $error !== null) {
            $this->failureTransport->send($envelope->with(
                ErrorDetailsStamp::fromThrowable($error),
            ));
        }

        return false;
    }

    private function detectBootloader(): Closure
    {
        $dir = __DIR__;
        for ($i = 0; $i < 6; $i++) {
            $autoload = $dir.'/vendor/autoload.php';
            if (file_exists($autoload)) {
                return static function () use ($autoload): void {
                    require_once $autoload;
                };
            }
            $dir = dirname($dir);
        }

        return static function (): void {
        };
    }
}
