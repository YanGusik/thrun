<?php

declare(strict_types=1);

namespace Thrun\Transport;

use Thrun\Contract\ReceiverInterface;
use Thrun\Contract\SchedulingStrategyInterface;
use Thrun\Envelope\Envelope;
use Thrun\Transport\Stamp\QueueStamp;
use Thrun\Transport\Strategy\RoundRobinStrategy;

final class MultiQueueReceiver implements ReceiverInterface
{
    private readonly SchedulingStrategyInterface $strategy;

    /** @var array<string, int> */
    private array $activeCount = [];

    /**
     * @param array<string, ReceiverInterface> $receivers
     * @param array<string, int>               $priorities queue name => priority (higher = preferred)
     */
    public function __construct(
        private readonly array $receivers,
        ?SchedulingStrategyInterface $strategy = null,
        private readonly array $priorities = [],
    ) {
        $this->strategy = $strategy ?? new RoundRobinStrategy();
    }

    public function receive(): ?Envelope
    {
        $result = $this->tryReceiveFromQueues();

        if ($result !== null) {
            return $result;
        }

        // all queues empty - block on preferred queue
        $preferredQueue = $this->strategy->next($this->buildQueueStates()) ?? array_key_first($this->receivers);
        $envelope       = $this->receivers[$preferredQueue]->receive();

        if ($envelope === null) {
            return null;
        }

        $this->activeCount[$preferredQueue] = ($this->activeCount[$preferredQueue] ?? 0) + 1;
        return $envelope->with(new QueueStamp($preferredQueue));
    }

    public function tryReceive(): ?Envelope
    {
        return $this->tryReceiveFromQueues();
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

    /** @return QueueState[] */
    private function buildQueueStates(): array
    {
        return array_map(
            fn(string $name) => new QueueState(
                name:     $name,
                priority: $this->priorities[$name] ?? 0,
                active:   $this->activeCount[$name] ?? 0,
            ),
            array_keys($this->receivers),
        );
    }

    /**
     * @param  QueueState[] $states
     * @return string[]
     */
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
