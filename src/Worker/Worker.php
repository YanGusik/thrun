<?php

declare(strict_types=1);

namespace Thrun\Worker;

use Async\ThreadChannel;
use Async\ThreadChannelException;
use Thrun\Contract\ReceiverInterface;
use Thrun\Envelope\Envelope;
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
                    /** @var array{ok: bool, envelope: Envelope} $result */
                    $result = $resultChannel->recv();
                } catch (ThreadChannelException|\Async\Cancellation) {
                    break;
                }

                if ($result['ok']) {
                    $this->transport->ack($result['envelope']);
                } else {
                    $this->transport->reject($result['envelope']);
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
            
            \Async\protect(function () use ($scope, $jobChannel, $threads, $resultChannel): void {
                $scope->cancel();
                
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
