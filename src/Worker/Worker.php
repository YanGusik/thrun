<?php

declare(strict_types=1);

namespace Thrun\Worker;


use Async\Scope;
use Async\TaskSet;
use Async\ThreadChannel;
use Async\ThreadPool;
use Async\ThreadPoolException;
use Closure;
use Throwable;
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
                /** @var array{ok: bool, envelope: Envelope, timedOut?: bool, error?: Throwable|null, processingTime?: float, wasRetried?: bool, retryDelayMs?: int|null} $result */
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

    private function handleFailure(Envelope $envelope, ?Throwable $error, ?int $explicitRetryDelayMs): bool
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
