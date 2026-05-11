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
     * @param array<class-string, callable(object): void> $handlers
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
                    try {
                        $timeoutStamp = $envelope->last(TimeoutStamp::class);
                        $timeoutMs = $timeoutStamp instanceof TimeoutStamp ? $timeoutStamp->timeoutMs : 0;

                        if ($timeoutMs > 0) {
                            $this->runWithTimeout($envelope, $timeoutMs);
                        } else {
                            $this->runHandler($envelope);
                            $this->resultChannel->send(['ok' => true, 'envelope' => $envelope]);
                        }
                    } catch (\Throwable $e) {
                        $this->resultChannel->send([
                            'ok' => false,
                            'envelope' => $envelope,
                            'timedOut' => false,
                            'error' => $e,
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

    private function runWithTimeout(Envelope $envelope, int $timeoutMs): void
    {
        $handlerScope = new \Async\Scope();
        $future = $handlerScope->spawn(function () use ($envelope): void {
            $this->runHandler($envelope);
        });

        try {
            await($future, \Async\timeout($timeoutMs));
            $this->resultChannel->send(['ok' => true, 'envelope' => $envelope]);
        } catch (OperationCanceledException $e) {
            $handlerScope->asNotSafely()->cancel();
            delay(50); // give finally blocks a chance to run
            $this->resultChannel->send([
                'ok' => false,
                'envelope' => $envelope,
                'timedOut' => true,
                'error' => new TimeoutException($timeoutMs),
            ]);
        } catch (\Throwable $e) {
            $handlerScope->asNotSafely()->cancel();
            $this->resultChannel->send([
                'ok' => false,
                'envelope' => $envelope,
                'timedOut' => false,
                'error' => $e,
            ]);
        }
    }

    private function runHandler(Envelope $envelope): void
    {
        $message = $envelope->message;
        $handler = $this->handlers[$message::class] ?? null;

        if ($handler === null) {
            throw new \RuntimeException(
                sprintf('No handler for "%s"', $message::class),
            );
        }

        $handler($message, $envelope);
    }
}
