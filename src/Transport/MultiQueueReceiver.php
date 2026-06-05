<?php

declare(strict_types=1);

namespace Thrun\Transport;

use Async\Scope;
use Thrun\Contract\ReceiverInterface;
use Thrun\Contract\SchedulingStrategyInterface;
use Thrun\Envelope\Envelope;
use Thrun\Envelope\Stamp\QueueStamp;
use Thrun\Transport\Strategy\RoundRobinStrategy;

final class MultiQueueReceiver implements ReceiverInterface
{
    private readonly SchedulingStrategyInterface $strategy;

    /** @var array<string, int> */
    private array $activeCount = [];

    /** @var array<string, \SplQueue<Envelope>> */
    private array $buffers = [];

    private readonly \Async\Channel $notifications;
    private ?\Async\Scope $backgroundScope = null;

    /**
     * @param array<string, ReceiverInterface> $receivers
     * @param array<string, int>               $priorities queue name => priority (higher = preferred)
     */
    public function __construct(
        private readonly array $receivers,
        ?SchedulingStrategyInterface $strategy = null,
        private readonly array $priorities = [],
    ) {
        $this->strategy      = $strategy ?? new RoundRobinStrategy();
        $this->notifications = new \Async\Channel(256);
        foreach (array_keys($this->receivers) as $name) {
            $this->buffers[$name] = new \SplQueue();
        }
    }

    public function receive(): ?Envelope
    {
        $this->ensureWatchers();

        while (true) {
            $envelope = $this->pickFromBuffers();
            if ($envelope !== null) {
                return $envelope;
            }

            try {
                $queueName = $this->notifications->recv();
                if ($queueName === null) {
                    return null; // Shutdown
                }
                // After notification, loop back to pickFromBuffers
            } catch (\Async\ChannelException|\Async\AsyncCancellation) {
                return null;
            }
        }
    }

    public function tryReceive(): ?Envelope
    {
        $envelope = $this->pickFromBuffers();
        if ($envelope !== null) {
            return $envelope;
        }

        return $this->tryReceiveFromQueues();
    }

    private function pickFromBuffers(): ?Envelope
    {
        $readyQueues = [];
        foreach ($this->buffers as $name => $queue) {
            if (!$queue->isEmpty()) {
                $readyQueues[] = $name;
            }
        }

        if ($readyQueues === []) {
            return null;
        }

        // Filter states only for queues that have data
        $states = array_filter(
            $this->buildQueueStates(),
            fn(QueueState $s) => in_array($s->name, $readyQueues, true)
        );

        $bestQueue = $this->strategy->next($states);
        if ($bestQueue === null || !isset($this->buffers[$bestQueue]) || $this->buffers[$bestQueue]->isEmpty()) {
            return null;
        }

        $envelope = $this->buffers[$bestQueue]->dequeue();
        $this->activeCount[$bestQueue] = ($this->activeCount[$bestQueue] ?? 0) + 1;
        
        return $envelope->with(new QueueStamp($bestQueue));
    }

    public function ack(Envelope $envelope): void
    {
        $stamp = $envelope->last(QueueStamp::class);
        if ($stamp instanceof QueueStamp && isset($this->receivers[$stamp->queue])) {
            $this->activeCount[$stamp->queue] = max(0, ($this->activeCount[$stamp->queue] ?? 1) - 1);
            $this->receivers[$stamp->queue]->ack($envelope);
        }
    }

    public function reject(Envelope $envelope): void
    {
        $stamp = $envelope->last(QueueStamp::class);
        if ($stamp instanceof QueueStamp && isset($this->receivers[$stamp->queue])) {
            $this->activeCount[$stamp->queue] = max(0, ($this->activeCount[$stamp->queue] ?? 1) - 1);
            $this->receivers[$stamp->queue]->reject($envelope);
        }
    }

    public function close(): void
    {
        echo "CLOSE CALLED IN MultiQueueReceiver.php\n";
        $this->backgroundScope?->asNotSafely()->cancel();
        $this->notifications->close();
    }

    private function ensureWatchers(): void
    {
        if ($this->backgroundScope !== null) {
            return;
        }

        $this->backgroundScope = Scope::inherit();
        foreach ($this->receivers as $name => $receiver) {
            $this->backgroundScope->spawn(function () use ($name, $receiver): void {
                while (true) {
                    try {
                        $envelope = $receiver->receive();
                        if ($envelope === null) {
                            $this->notifications->send(null);
                            break;
                        }
                        $this->buffers[$name]->enqueue($envelope);
                        $this->notifications->send($name);
                    } catch (\Async\AsyncCancellation) {
                        break;
                    } catch (\Throwable) {
                        $this->notifications->send(null);
                        break;
                    }
                }
            });
        }
    }

    private function tryReceiveFromQueues(): ?Envelope
    {
        $states         = $this->buildQueueStates();
        $preferredQueue = $this->strategy->next($states);

        foreach ($this->fallbackOrder($states, $preferredQueue) as $queue) {
            $envelope = $this->receivers[$queue]->tryReceive();

            if ($envelope !== null) {
                $this->activeCount[$queue] = ($this->activeCount[$queue] ?? 0) + 1;
                return $envelope->with(new QueueStamp($queue));
            }
        }

        return null;
    }

    private function buildQueueStates(): array
    {
        $states = [];
        foreach ($this->receivers as $name => $_) {
            $states[] = new QueueState(
                name:     $name,
                priority: $this->priorities[$name] ?? 0,
                active:   $this->activeCount[$name] ?? 0,
            );
        }
        return $states;
    }

    private function fallbackOrder(array $states, ?string $preferredQueue): array
    {
        $order = $preferredQueue !== null ? [$preferredQueue] : [];
        foreach ($states as $state) {
            if ($state->name !== $preferredQueue) {
                $order[] = $state->name;
            }
        }
        return $order;
    }
}
