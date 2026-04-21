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

        $capacity      = $this->options->threads * $this->options->concurrency;
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
                    new WorkerThread($jobChannel, $resultChannel, $handlers, $concurrency)->run();
                },
                bootloader: $bootloader,
            );
        }

        // read results and ack/reject transport
        $resultReader = spawn(function () use ($resultChannel): void {
            while (true) {
                try {
                    /** @var array{ok: bool, envelope: Envelope} $result */
                    $result = $resultChannel->recv();
                } catch (ThreadChannelException) {
                    break;
                }

                if ($result['ok']) {
                    $this->transport->ack($result['envelope']);
                } else {
                    $this->transport->reject($result['envelope']);
                }
            }
        });

        // producer loop
        while ($this->running) {
            $envelope = $this->transport->receive();

            if ($envelope === null) {
                break;
            }

            $jobChannel->send($envelope);
        }

        // signal threads to stop and wait for them
        $jobChannel->close();

        foreach ($threads as $thread) {
            await($thread);
        }

        // all threads done and finish reader
        $resultChannel->close();
        await($resultReader);
    }

    public function stop(): void
    {
        $this->running = false;
    }

    private function detectBootloader(): \Closure
    {
        // Walk up from this file to find vendor/autoload.php
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
