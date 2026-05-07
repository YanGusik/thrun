<?php

declare(strict_types=1);

namespace Thrun\Worker;

use Async\OperationCanceledException;
use Async\ThreadChannel;
use Async\ThreadChannelException;
use Thrun\Envelope\Envelope;
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
                        $message = $envelope->message;
                        $handler = $this->handlers[$message::class] ?? null;

                        if ($handler === null) {
                            throw new \RuntimeException(
                                sprintf('No handler for "%s"', $message::class),
                            );
                        }

                        $handler($message);

                        // Use blocking send for backpressure, it's safe in TaskGroup
                        $this->resultChannel->send(['ok' => true, 'envelope' => $envelope]);
                    } catch (\Throwable) {
                        $this->resultChannel->send(['ok' => false, 'envelope' => $envelope]);
                    }
                });
            }
        } catch (ThreadChannelException|OperationCanceledException) {
            // closed or cancelled
        } finally {
            $group->seal();
            try {
                // Wait for remaining tasks
                $group->awaitCompletion();
            } catch (\Throwable) {
                // ignore
            }
        }
    }
}
