<?php

declare(strict_types=1);

namespace Thrun\Tests\Unit\Transport;

use Testo\Assert;
use Thrun\Envelope\Envelope;
use Thrun\Envelope\Stamp\PartitionStamp;
use Thrun\Tests\AsyncTestCase;
use Thrun\Tests\Fixture\PingMessage;
use Thrun\Transport\InMemory\InMemoryTransport;
use Thrun\Transport\Policy\ChainPolicy;
use Thrun\Transport\Policy\MaxConcurrencyPolicy;
use Thrun\Transport\PolicyAwareReceiver;

final class ChainPolicyTest extends AsyncTestCase
{
    public function allowsWhenAllPoliciesAllow(): void
    {
        $transport = new InMemoryTransport();
        $transport->send(Envelope::wrap(new PingMessage())->with(new PartitionStamp('user1')));
        $transport->close();

        $receiver = new PolicyAwareReceiver(
            inner:  $transport,
            policy: new ChainPolicy([
                new MaxConcurrencyPolicy(maxPerPartition: 2),
                new MaxConcurrencyPolicy(maxPerPartition: 5),
            ]),
        );

        Assert::same($receiver->receive() !== null, true);
    }

    public function deniesWhenAnyPolicyDenies(): void
    {
        $transport = new InMemoryTransport();

        $msg1 = Envelope::wrap(new PingMessage())->with(new PartitionStamp('user1'));
        $msg2 = Envelope::wrap(new PingMessage())->with(new PartitionStamp('user1'));
        $msg3 = Envelope::wrap(new PingMessage())->with(new PartitionStamp('user2'));

        $transport->send($msg1);
        $transport->send($msg2);
        $transport->send($msg3);
        $transport->close();

        // first policy: max 1 per partition
        // second policy: max 5 per partition (allows everything)
        // combined: max 1 wins
        $receiver = new PolicyAwareReceiver(
            inner:  $transport,
            policy: new ChainPolicy([
                new MaxConcurrencyPolicy(maxPerPartition: 1),
                new MaxConcurrencyPolicy(maxPerPartition: 5),
            ]),
        );

        $e1 = $receiver->receive();
        Assert::same($e1->last(PartitionStamp::class)->key, 'user1');

        // user1 at limit → msg2 buffered, msg3 from user2 allowed
        $e2 = $receiver->receive();
        Assert::same($e2->last(PartitionStamp::class)->key, 'user2');

        $receiver->ack($e1);

        $e3 = $receiver->receive();
        Assert::same($e3->last(PartitionStamp::class)->key, 'user1');
    }

    public function releasesAllPoliciesOnAck(): void
    {
        $transport = new InMemoryTransport();

        $msg1 = Envelope::wrap(new PingMessage())->with(new PartitionStamp('user1'));
        $msg2 = Envelope::wrap(new PingMessage())->with(new PartitionStamp('user1'));

        $transport->send($msg1);
        $transport->send($msg2);
        $transport->close();

        $receiver = new PolicyAwareReceiver(
            inner:  $transport,
            policy: new ChainPolicy([
                new MaxConcurrencyPolicy(maxPerPartition: 1),
            ]),
        );

        $e1 = $receiver->receive();
        $receiver->ack($e1);

        // slot released in all policies - msg2 now allowed
        $e2 = $receiver->receive();
        Assert::same($e2->last(PartitionStamp::class)->key, 'user1');
    }
}
