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
    private readonly Channel $internalChannel;
    private ?\Async\Scope $backgroundScope = null;

    public function __construct(
        private readonly ReceiverInterface       $inner,
        private readonly DispatchPolicyInterface $policy,
    ) {
        $this->released        = new Channel(128);
        $this->internalChannel = new Channel(256);
    }

    public function receive(): ?Envelope
    {
        $this->ensureWatchers();

        while (true) {
            foreach ($this->pending as $i => $envelope) {
                if ($this->policy->allows($envelope)) {
                    array_splice($this->pending, $i, 1);
                    $this->policy->acquire($envelope);
                    return $envelope;
                }
            }

            try {
                $data = $this->internalChannel->recv();
                if ($data === null) {
                    return null;
                }

                [$type, $val] = $data;

                if ($type === 'inner') {
                    if ($val === null) {
                        return null;
                    }
                    if ($this->policy->allows($val)) {
                        $this->policy->acquire($val);
                        return $val;
                    }
                    $this->pending[] = $val;
                }
            } catch (ChannelException|OperationCanceledException) {
                return null;
            }
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

    private function ensureWatchers(): void
    {
        if ($this->backgroundScope !== null) {
            return;
        }

        $this->backgroundScope = new \Async\Scope();

        // Release watcher
        $this->backgroundScope->spawn(function (): void {
            while (true) {
                try {
                    $this->released->recv();
                    $this->internalChannel->send(['released', null]);
                } catch (OperationCanceledException) {
                    break;
                } catch (\Throwable) {
                    break;
                }
            }
        });

        // Inner watcher
        $this->backgroundScope->spawn(function (): void {
            while (true) {
                try {
                    $envelope = $this->inner->receive();
                    $this->internalChannel->send(['inner', $envelope]);
                    if ($envelope === null) {
                        break;
                    }
                } catch (OperationCanceledException) {
                    break;
                } catch (\Throwable) {
                    try {
                        $this->internalChannel->send(['inner', null]);
                    } catch (\Throwable) {}
                    break;
                }
            }
        });
    }
}
