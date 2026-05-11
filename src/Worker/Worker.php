<?php

declare(strict_types=1);

namespace Thrun\Worker;

use Async\ThreadChannel;
use Async\ThreadChannelException;
use Thrun\Contract\ReceiverInterface;
use Thrun\Contract\SenderInterface;
use Thrun\Envelope\Envelope;
use Thrun\Envelope\Stamp\DelayStamp;
use Thrun\Envelope\Stamp\ErrorDetailsStamp;
use Thrun\Envelope\Stamp\RetryStamp;
use Thrun\Worker\Retry\NoRetryStrategy;
use function Async\await;
use function Async\spawn;
use function Async\spawn_thread;

final class Worker
{
    private bool $running = false;

    /**
     * @param array<class-string, callable(object): void> $handlers
     */
    public function __construct(
        private readonly ReceiverInterface $transport,
        private readonly array             $handlers,
        private readonly WorkerOptions     $options = new WorkerOptions(),
        private readonly ?SenderInterface  $failureTransport = null,
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

        $scope = new \Async\Scope();

        // result reader coroutine
        $scope->spawn(function () use ($resultChannel): void {
            while (true) {
                try {
                    /** @var array{ok: bool, envelope: Envelope, timedOut?: bool, error?: \Throwable|null} $result */
                    $result = $resultChannel->recv();
                } catch (ThreadChannelException|\Async\Cancellation) {
                    break;
                }

                if ($result['ok']) {
                    $this->transport->ack($result['envelope']);
                } else {
                    $this->handleFailure($result['envelope'], $result['error'] ?? null);
                }
            }
        });

        // producer loop coroutine
        $producer = $scope->spawn(function () use ($jobChannel): void {
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
            
            \Async\protect(function () use ($jobChannel, $threads, $resultChannel, $scope): void {
                $jobChannel->close();
                foreach ($threads as $thread) {
                    try {
                        await($thread);
                    } catch (\Throwable) {}
                }

                $resultChannel->close();
                try {
                    $scope->awaitCompletion(\Async\timeout(2000));
                } catch (\Throwable) {}
            });
        }
    }

    public function stop(): void
    {
        $this->running = false;
    }

    private function handleFailure(Envelope $envelope, ?\Throwable $error): void
    {
        $retryStamp = $envelope->last(RetryStamp::class);
        $strategy = $retryStamp?->strategy ?? new NoRetryStrategy();
        $attempt = $retryStamp?->attempts ?? 0;

        if ($strategy->isRetryable($error ?? new \RuntimeException('Unknown error'), $attempt + 1)) {
            $this->transport->reject($envelope);
            $this->transport->send($envelope->with(
                new RetryStamp(attempts: $attempt + 1, strategy: $strategy),
                new DelayStamp(delayMs: $strategy->getDelay($attempt + 1)),
            ));
        } else {
            $this->transport->reject($envelope);
            if ($this->failureTransport !== null && $error !== null) {
                $this->failureTransport->send($envelope->with(
                    ErrorDetailsStamp::fromThrowable($error),
                ));
            }
        }
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
