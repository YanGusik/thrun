<?php

declare(strict_types=1);

namespace Thrun\Tests\Unit\Envelope;

use Testo\Assert;
use Thrun\Envelope\Envelope;
use Thrun\Envelope\Stamp\JobIdStamp;
use Thrun\Envelope\Stamp\PartitionStamp;
use Thrun\Tests\AsyncTestCase;
use Thrun\Tests\Fixture\PingMessage;
use Thrun\Envelope\Stamp\QueueStamp;

final class EnvelopeTest extends AsyncTestCase
{
    public function wrapCreatesEnvelope(): void
    {
        $message  = new PingMessage();
        $envelope = Envelope::wrap($message);

        Assert::same($envelope->message, $message);
    }

    public function withAddsStamp(): void
    {
        $envelope = Envelope::wrap(new PingMessage())
            ->with(new PartitionStamp('user1'));

        Assert::same($envelope->last(PartitionStamp::class)?->key, 'user1');
    }

    public function withIsImmutable(): void
    {
        $original = Envelope::wrap(new PingMessage());
        $modified = $original->with(new PartitionStamp('user1'));

        Assert::same($original->has(PartitionStamp::class), false);
        Assert::same($modified->has(PartitionStamp::class), true);
    }

    public function lastReturnsLatestStamp(): void
    {
        $envelope = Envelope::wrap(new PingMessage())
            ->with(new PartitionStamp('first'))
            ->with(new PartitionStamp('second'));

        Assert::same($envelope->last(PartitionStamp::class)?->key, 'second');
    }

    public function lastReturnsNullWhenMissing(): void
    {
        $envelope = Envelope::wrap(new PingMessage());
        Assert::same($envelope->last(PartitionStamp::class), null);
    }

    public function allReturnsAllStampsOfType(): void
    {
        $envelope = Envelope::wrap(new PingMessage())
            ->with(new PartitionStamp('a'))
            ->with(new PartitionStamp('b'));

        $stamps = $envelope->all(PartitionStamp::class);
        Assert::same(count($stamps), 2);
        Assert::same($stamps[0]->key, 'a');
        Assert::same($stamps[1]->key, 'b');
    }

    public function hasReturnsTrueWhenStampPresent(): void
    {
        $envelope = Envelope::wrap(new PingMessage())
            ->with(new PartitionStamp('user1'));

        Assert::same($envelope->has(PartitionStamp::class), true);
        Assert::same($envelope->has(QueueStamp::class), false);
    }

    public function multipleStampTypes(): void
    {
        $envelope = Envelope::wrap(new PingMessage())
            ->with(new PartitionStamp('user1'))
            ->with(new QueueStamp('emails'));

        Assert::same($envelope->last(PartitionStamp::class)?->key, 'user1');
        Assert::same($envelope->last(QueueStamp::class)?->queue, 'emails');
    }

    public function allStampsReturnsWithJobIdStamp(): void
    {
        $envelope = Envelope::wrap(new PingMessage())
            ->with(new PartitionStamp('a'))
            ->with(new PartitionStamp('b'))
            ->with(new QueueStamp('emails'));

        $all = $envelope->allStamps();
        var_dump($all);
        Assert::same(count($all), 4);
        Assert::notNull($envelope->last(JobIdStamp::class)?->id);
    }
}
