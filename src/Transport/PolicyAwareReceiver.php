<?php

declare(strict_types=1);

namespace Thrun\Transport;

use Async\Channel;
use Async\ChannelException;
use Async\OperationCanceledException;
use Thrun\Contract\DispatchPolicyInterface;
use Thrun\Contract\ReceiverInterface;
use Thrun\Envelope\Envelope;

final class PolicyAwareReceiver implements ReceiverInterface
{
    /** @var Envelope[] */
    private array $pending = [];

    private readonly Channel $released;

    public function __construct(
        private readonly ReceiverInterface       $inner,
        private readonly DispatchPolicyInterface $policy,
    ) {
        $this->released = new Channel(1);
    }

    public function receive(): ?Envelope
    {
        while (true) {
            foreach ($this->pending as $i => $envelope) {
                if ($this->policy->allows($envelope)) {
                    array_splice($this->pending, $i, 1);
                    $this->policy->acquire($envelope);
                    return $envelope;
                }
            }

            if (!empty($this->pending)) {
                $envelope = $this->inner->tryReceive();

                if ($envelope === null) {
                    try {
                        $this->released->recv();
                    } catch (ChannelException|OperationCanceledException) {
                        return null;
                    }
                    continue;
                }
            } else {
                $envelope = $this->inner->receive();

                if ($envelope === null) {
                    return null;
                }
            }

            if ($this->policy->allows($envelope)) {
                $this->policy->acquire($envelope);
                return $envelope;
            }

            $this->pending[] = $envelope;
        }
    }

    public function tryReceive(): ?Envelope
    {
        foreach ($this->pending as $i => $envelope) {
            if ($this->policy->allows($envelope)) {
                array_splice($this->pending, $i, 1);
                $this->policy->acquire($envelope);
                return $envelope;
            }
        }

        $envelope = $this->inner->tryReceive();

        if ($envelope === null) {
            return null;
        }

        if ($this->policy->allows($envelope)) {
            $this->policy->acquire($envelope);
            return $envelope;
        }

        $this->pending[] = $envelope;
        return null;
    }

    public function ack(Envelope $envelope): void
    {
        $this->policy->release($envelope);
        $this->inner->ack($envelope);
        $this->released->sendAsync(true);
    }

    public function reject(Envelope $envelope): void
    {
        $this->policy->release($envelope);
        $this->inner->reject($envelope);
        $this->released->sendAsync(true);
    }
}
