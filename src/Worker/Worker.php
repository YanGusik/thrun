<?php

declare(strict_types=1);

namespace Thrun\Worker;

use Async\ThreadChannel;
use Async\ThreadChannelException;
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
use function Async\spawn;
use function Async\spawn_thread;

final class Worker
{
    private bool $running = false;

    /**
     * @param array<class-string, callable(object, ?Acknowledger): void> $handlers
     */
    public function __construct(
        private readonly ReceiverInterface $transport,
        private readonly array             $handlers,
        private readonly WorkerOptions     $options = new WorkerOptions(),
        private readonly ?SenderInterface  $failureTransport = null,
        private readonly MetricsInterface  $metrics = new NullMetrics(),
    ) {}

    public function run(): void
    {
        $this->running = true;

        // Larger capacity to prevent trivial deadlocks
        $capacity      = max(128, $this->options->threads * $this->options->concurrency * 2);
        $jobChannel    = new ThreadChannel($capacity);
        $resultChannel = new ThreadChannel($capacity);

        $handlers    = $this->handlers;
        $concurrency = $this->options->concurrency;
        $bootloader  = $this->options->bootloader ?? $this->detectBootloader();

        // spawn N worker threads
        $threads = [];
        for ($i = 0; $i < $this->options->threads; $i++) {
            $threads[] = spawn_thread(
                static function () use ($jobChannel, $resultChannel, $handlers, $concurrency): void {
                    (new WorkerThread($jobChannel, $resultChannel, $handlers, $concurrency))->run();
                },
                bootloader: $bootloader,
            );
        }

        $this->scope = new \Async\Scope();

        // result reader coroutine
        $this->scope->spawn(function () use ($resultChannel): void {
            while (true) {
                try {
                    /** @var array{ok: bool, envelope: Envelope, timedOut?: bool, error?: \Throwable|null, processingTime?: float, wasRetried?: bool, retryDelayMs?: int|null} $result */
                    $result = $resultChannel->recv();
                } catch (ThreadChannelException|\Async\Cancellation) {
                    break;
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
        });

        // producer loop coroutine
        $producer = $this->scope->spawn(function () use ($jobChannel): void {
            while ($this->running) {
                try {
                    $envelope = $this->transport->receive();
                    if ($envelope === null) {
                        break;
                    }
                    $jobChannel->send($envelope);
                } catch (\Async\Cancellation) {
                    break;
                } catch (\Throwable) {
                    break;
                }
            }
        });

        try {
            await($producer);
        } catch (\Async\Cancellation) {
            // normal stop
        } finally {
            $this->running = false;
            
            \Async\protect(function () use ($jobChannel, $threads, $resultChannel): void {
                $jobChannel->close();
                foreach ($threads as $thread) {
                    try {
                        await($thread);
                    } catch (\Throwable) {}
                }

                $resultChannel->close();
                try {
                    $this->scope?->awaitCompletion(\Async\timeout(2000));
                } catch (\Throwable) {
                    $this->scope?->dispose();
                }
            });
        }
    }

    public function stop(): void
    {
        $this->running = false;
        $this->scope?->cancel();
        $this->transport->close();
    }

    private function handleFailure(Envelope $envelope, ?\Throwable $error, ?int $explicitRetryDelayMs): bool
    {
        $retryStamp = $envelope->last(RetryStamp::class);
        $strategy = $retryStamp?->strategy ?? new NoRetryStrategy();
        $attempt = $retryStamp?->attempts ?? 0;

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

    private function detectBootloader(): \Closure
    {
        $dir = __DIR__;
        for ($i = 0; $i < 6; $i++) {
            $autoload = $dir . '/vendor/autoload.php';
            if (file_exists($autoload)) {
                return static function () use ($autoload): void {
                    require_once $autoload;
                };
            }
            $dir = dirname($dir);
        }

        return static function (): void {};
    }
}
