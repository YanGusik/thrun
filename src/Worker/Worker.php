<?php

declare(strict_types=1);

namespace Thrun\Worker;


use Async\Scope;
use Async\TaskSet;
use Async\ThreadChannel;
use Async\ThreadPool;
use Closure;
use Throwable;
use Thrun\Contract\MetricsInterface;
use Thrun\Contract\ReceiverInterface;
use Thrun\Contract\SenderInterface;
use Thrun\Envelope\Envelope;
use Thrun\Envelope\Stamp\DelayStamp;
use Thrun\Envelope\Stamp\ErrorDetailsStamp;
use Thrun\Envelope\Stamp\JobIdStamp;
use Thrun\Envelope\Stamp\RedeliveryStamp;
use Thrun\Envelope\Stamp\RetryStamp;
use Thrun\Worker\Metrics\NullMetrics;

final class Worker
{
    private bool $running = false;
    private Scope $mainScope;
    private ThreadPool $threadPool;
    private TaskSet $taskSet;
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
        $this->mainScope     = new Scope();
        $capacity            = max(128, $this->options->threads * $this->options->concurrency * 2);
        $this->resultChannel = new ThreadChannel($capacity);

        $bootloader = $this->options->bootloader ?? $this->detectBootloader();

        $this->threadPool = new ThreadPool(
            workers: $this->options->threads,
            queueSize: $this->options->queueSize,
            bootloader: $bootloader,
            coroutine: $this->options->concurrency > 0,
            concurrency: $this->options->concurrency
        );
        $this->taskSet    = new TaskSet(concurrency: 3, scope: $this->mainScope);
    }

    public function run(): void
    {
        $this->running = true;
        try {
            $this->taskSet->spawn($this->resultReaderCoro());
            $this->taskSet->spawn($this->producerCoro());

            foreach ($this->taskSet as [$result, $error]) {
                if ($error !== null) {
                    /** @var Throwable $error */
                    if ($error instanceof \Cancellation) {
                        continue;
                    }
                    throw $error;
                    $msg = sprintf("[Worker] %s: %s\n[stacktrace]\n%s", get_class($error), $error->getMessage(),
                        $error->getTraceAsString());
                    error_log($msg);
                }
            }

        } finally {
            $this->stop();
        }
    }

    public function stop(): void
    {
        if (!$this->running) {
            return;
        }
        $this->threadPool->close();
        $this->mainScope->asNotSafely()->cancel();
        $this->taskSet->cancel();
        $this->running = false;
        $this->resultChannel?->close();
    }

    public function isIdle(): bool
    {
        return $this->threadPool->getPendingCount() === 0
            && $this->threadPool->getRunningCount() === 0
            && $this->threadPool->getCompletedCount() > 0;
    }

    private function producerCoro(): Closure
    {
        return function (): void {
            $handlers      = $this->handlers;
            $middleware    = $this->options->middleware;
            $resultChannel = $this->resultChannel;

            while ($this->running) {
                $envelope = $this->transport->receive();
                if ($envelope === null) {
                    break;
                }

                if (!$envelope->has(JobIdStamp::class)) {
                    $envelope = $envelope->with(new JobIdStamp());
                }

//                var_dump($envelope, $resultChannel, $handlers, $middleware);

                $this->threadPool->submit(static function () use ($envelope, $resultChannel, $handlers, $middleware) {
                    new WorkerThread($envelope, $resultChannel, $handlers, $middleware)->run();
                });


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
                /** @var array{ok: bool, envelope: Envelope, timedOut?: bool, error?: array{class:string,message:string,code:int,trace:string}|Throwable|null, processingTime?: float, wasRetried?: bool, retryDelayMs?: int|null} $result */
                $result = $this->resultChannel->recv();

//                protect(function () use ($result) {
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

                        $error      = null;
                        $errorStamp = null;
                        if (isset($result['error']['class'])) {
                            $error      = new \RuntimeException(
                                sprintf('[%s] %s', $result['error']['class'], $result['error']['message']),
                                $result['error']['code'] ?? 0,
                            );
                            $errorStamp = new ErrorDetailsStamp(
                                exceptionClass: $result['error']['class'],
                                message: $result['error']['message'],
                                code: $result['error']['code'] ?? 0,
                                trace: $result['error']['trace'],
                                file: $result['error']['file'] ?? null,
                                line: $result['error']['line'] ?? null,
                            );
                        }

                        $wasRetried = $this->handleFailure(
                            $result['envelope'],
                            $error,
                            $errorStamp,
                            $result['retryDelayMs'] ?? null,
                        );

                        if ($wasRetried) {
                            $this->metrics->incrementRetried();
                        }
                    }
//                });
            }
        };
    }

    private function handleFailure(
        Envelope $envelope, ?Throwable $error, ?ErrorDetailsStamp $errorStamp, ?int $explicitRetryDelayMs
    ): bool {
        $retryStamp   = $envelope->last(RetryStamp::class);
        $redeliveries = $envelope->all(RedeliveryStamp::class);
        $attempt      = array_last($redeliveries)->attempt ?? 0;

        // Explicit retry from handler takes priority over stamp strategy
        if ($explicitRetryDelayMs !== null) {
            $this->transport->reject($envelope);
            $this->transport->send(
                $this->buildRetryEnvelope($envelope, $explicitRetryDelayMs, $attempt + 1),
            );

            return true;
        }

        if ($retryStamp instanceof RetryStamp && $retryStamp->isRetryable($attempt + 1)) {
            $delayMs = $retryStamp->getDelayForAttempt($attempt + 1);

            $this->transport->reject($envelope);
            $this->transport->send(
                $this->buildRetryEnvelope($envelope, $delayMs, $attempt + 1),
            );

            return true;
        }

        $this->transport->reject($envelope);
        if ($this->failureTransport !== null && $errorStamp !== null) {
            $this->failureTransport->send($envelope->with($errorStamp));
        }

        return false;
    }

    private function buildRetryEnvelope(Envelope $envelope, int $delayMs, int $nextAttempt): Envelope
    {
        $history   = $envelope->all(RedeliveryStamp::class);
        $history[] = new RedeliveryStamp(
            attempt: $nextAttempt,
            retriedAt: (new \DateTimeImmutable())->format(\DateTimeInterface::ATOM),
        );

        // Symfony-style history capping: keep first + last 9 (10 total)
        if (count($history) > 10) {
            $history = array_merge([$history[0]], array_slice($history, -9));
        }

        return $envelope
            ->withoutAll(RedeliveryStamp::class)
            ->withoutAll(JobIdStamp::class)
            ->with(...$history)
            ->with(new DelayStamp(delayMs: $delayMs))
            ->with(new JobIdStamp());
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
