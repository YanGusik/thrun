<?php

declare(strict_types=1);

namespace Thrun\Worker;

use Async\Channel;
use Async\OperationCanceledException;
use Async\ThreadChannel;
use Async\ThreadChannelException;
use Thrun\Envelope\Envelope;
use function Async\spawn;
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
        ini_set('memory_limit', '-1'); //TODO: temporary, after delete

        // semaphore logic
        $sem = new Channel($this->concurrency);

        for ($i = 0; $i < $this->concurrency; $i++) {
            $sem->sendAsync(1);
        }

        while (true) {
            $sem->recv();

            try {
                /** @var Envelope $envelope */
                $envelope = $this->jobChannel->recv();
            } catch (ThreadChannelException|OperationCanceledException) {
                $sem->sendAsync(1);
                break; // GO TO drain semaphore
            }

            spawn(function () use ($envelope, $sem): void {
                try {
                    $message = $envelope->message;
                    $handler = $this->handlers[$message::class] ?? null;

                    if ($handler === null) {
                        throw new \RuntimeException(
                            sprintf('No handler for "%s"', $message::class),
                        );
                    }

                    $handler($message);

                    $this->resultChannel->send(['ok' => true, 'envelope' => $envelope]);
                } catch (\Throwable) {
                    $this->resultChannel->send(['ok' => false, 'envelope' => $envelope]);
                } finally {
                    $sem->sendAsync(1);
                }
            });
        }

        // drain semaphore
        for ($i = 0; $i < $this->concurrency; $i++) {
            $sem->recv();
        }
    }
}
