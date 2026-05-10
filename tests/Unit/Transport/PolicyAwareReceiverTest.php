<?php

declare(strict_types=1);

namespace Thrun\Tests\Unit\Transport;

use Testo\Assert;
use Thrun\Envelope\Envelope;
use Thrun\Envelope\Stamp\PartitionStamp;
use Thrun\Tests\AsyncTestCase;
use Thrun\Tests\Fixture\PingMessage;
use Thrun\Transport\InMemory\InMemoryTransport;
use Thrun\Transport\Policy\MaxConcurrencyPolicy;
use Thrun\Transport\PolicyAwareReceiver;

final class PolicyAwareReceiverTest extends AsyncTestCase
{
    public function passesMessageWhenPolicyAllows(): void
    {
        $transport = new InMemoryTransport();
        $transport->send(Envelope::wrap(new PingMessage()));
        $transport->close();

        $receiver = new PolicyAwareReceiver(
            inner:  $transport,
            policy: new MaxConcurrencyPolicy(maxPerPartition: 1),
        );

        $envelope = $receiver->receive();
        Assert::same($envelope !== null, true);

        Assert::same($receiver->receive(), null);

        $receiver->close();
    }

    public function buffersMessageWhenConcurrencyLimitReached(): void
    {
        $transport = new InMemoryTransport();

        $msg1 = Envelope::wrap(new PingMessage())->with(new PartitionStamp('user1'));
        $msg2 = Envelope::wrap(new PingMessage())->with(new PartitionStamp('user1'));
        $msg3 = Envelope::wrap(new PingMessage())->with(new PartitionStamp('user2'));

        $transport->send($msg1);
        $transport->send($msg2);
        $transport->send($msg3);
        $transport->close();

        $receiver = new PolicyAwareReceiver(
            inner:  $transport,
            policy: new MaxConcurrencyPolicy(maxPerPartition: 1),
        );

        // msg1 allowed
        $e1 = $receiver->receive();
        Assert::same($e1->last(PartitionStamp::class)->key, 'user1');

        // msg2 denied (user1 at limit) → buffered, msg3 allowed
        $e2 = $receiver->receive();
        Assert::same($e2->last(PartitionStamp::class)->key, 'user2');

        // ack msg1 → frees user1 slot
        $receiver->ack($e1);

        // pending msg2 now allowed
        $e3 = $receiver->receive();
        Assert::same($e3->last(PartitionStamp::class)->key, 'user1');

        // transport empty and closed → null
        Assert::same($receiver->receive(), null);

        $receiver->close();
    }

    public function releaseOnReject(): void
    {
        $transport = new InMemoryTransport();

        $msg1 = Envelope::wrap(new PingMessage())->with(new PartitionStamp('user1'));
        $msg2 = Envelope::wrap(new PingMessage())->with(new PartitionStamp('user1'));

        $transport->send($msg1);
        $transport->send($msg2);
        $transport->close();

        $receiver = new PolicyAwareReceiver(
            inner:  $transport,
            policy: new MaxConcurrencyPolicy(maxPerPartition: 1),
        );

        $e1 = $receiver->receive();
        Assert::same($e1->last(PartitionStamp::class)->key, 'user1');

        // reject also releases the slot
        $receiver->reject($e1);

        $e2 = $receiver->receive();
        Assert::same($e2->last(PartitionStamp::class)->key, 'user1');

        $receiver->close();
    }
}
