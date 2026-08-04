<?php

declare(strict_types=1);

namespace Thrun\Tests\Unit\Transport;

use Testo\Assert;
use Thrun\Envelope\Envelope;
use Thrun\Tests\AsyncTestCase;
use Thrun\Tests\Fixture\PingMessage;
use Thrun\Transport\InMemory\InMemoryTransport;
use Thrun\Transport\MultiQueueReceiver;
use Thrun\Envelope\Stamp\QueueStamp;
use Thrun\Transport\Strategy\PriorityStrategy;

final class MultiQueueReceiverTest extends AsyncTestCase
{
    public function roundRobinAlternatesQueues(): void
    {
        $a = new InMemoryTransport();
        $b = new InMemoryTransport();

        $a->send(Envelope::wrap(new PingMessage()));
        $a->send(Envelope::wrap(new PingMessage()));
        $b->send(Envelope::wrap(new PingMessage()));
        $b->send(Envelope::wrap(new PingMessage()));

        $receiver = new MultiQueueReceiver(receivers: ['a' => $a, 'b' => $b]);

        Assert::same($receiver->tryReceive()?->last(QueueStamp::class)?->queue, 'a');
        Assert::same($receiver->tryReceive()?->last(QueueStamp::class)?->queue, 'b');
        Assert::same($receiver->tryReceive()?->last(QueueStamp::class)?->queue, 'a');
        Assert::same($receiver->tryReceive()?->last(QueueStamp::class)?->queue, 'b');

        $receiver->close();
    }

    public function roundRobinFallsBackWhenPreferredIsEmpty(): void
    {
        $a = new InMemoryTransport(); // empty
        $b = new InMemoryTransport();

        $b->send(Envelope::wrap(new PingMessage()));
        $b->send(Envelope::wrap(new PingMessage()));

        $receiver = new MultiQueueReceiver(receivers: ['a' => $a, 'b' => $b]);

        // strategy prefers A, A is empty - falls back to B
        Assert::same($receiver->tryReceive()?->last(QueueStamp::class)?->queue, 'b');
        Assert::same($receiver->tryReceive()?->last(QueueStamp::class)?->queue, 'b');

        $receiver->close();
    }

    public function priorityDeliversProportionally(): void
    {
        $emails        = new InMemoryTransport();
        $notifications = new InMemoryTransport();

        for ($i = 0; $i < 6; $i++) {
            $emails->send(Envelope::wrap(new PingMessage()));
        }
        for ($i = 0; $i < 2; $i++) {
            $notifications->send(Envelope::wrap(new PingMessage()));
        }

        $receiver = new MultiQueueReceiver(
            receivers: [
                'emails'        => $emails,
                'notifications' => $notifications,
            ],
            strategy:   new PriorityStrategy(),
            priorities: ['emails' => 3, 'notifications' => 1],
        );

        $order = [];
        for ($i = 0; $i < 8; $i++) {
            $order[] = $receiver->tryReceive()?->last(QueueStamp::class)?->queue;
        }

        Assert::same($order, [
            'emails', 'emails', 'emails', 'notifications',
            'emails', 'emails', 'emails', 'notifications',
        ]);

        $receiver->close();
    }

    public function priorityFallsBackWhenPreferredIsEmpty(): void
    {
        $emails        = new InMemoryTransport(); // empty
        $notifications = new InMemoryTransport();

        $notifications->send(Envelope::wrap(new PingMessage()));

        $receiver = new MultiQueueReceiver(
            receivers: [
                'emails'        => $emails,
                'notifications' => $notifications,
            ],
            strategy:   new PriorityStrategy(),
            priorities: ['emails' => 3, 'notifications' => 1],
        );

        // emails preferred but empty - falls back to notifications
        Assert::same($receiver->tryReceive()?->last(QueueStamp::class)?->queue, 'notifications');

        $receiver->close();
    }

    public function bufferedReceiveDeliversWhenOnlyALaterQueueHasWork(): void
    {
        $emails        = new InMemoryTransport(); // stays empty
        $notifications = new InMemoryTransport();

        $notifications->send(Envelope::wrap(new PingMessage()));

        $receiver = new MultiQueueReceiver(
            receivers: [
                'emails'        => $emails,
                'notifications' => $notifications,
            ],
            strategy:   new PriorityStrategy(),
            priorities: ['emails' => 3, 'notifications' => 1],
        );

        // The buffered path hands the strategy a filtered state list, and a
        // strategy that trips over its keys kills the producer coroutine.
        $this->withWarningsAsErrors(static function () use ($receiver): void {
            Assert::same($receiver->receive()?->last(QueueStamp::class)?->queue, 'notifications');
        });

        $receiver->close();
    }

    public function bufferedReceiveDeliversWhenOnlyTheFirstQueueHasWork(): void
    {
        $emails        = new InMemoryTransport();
        $notifications = new InMemoryTransport(); // stays empty

        $emails->send(Envelope::wrap(new PingMessage()));

        $receiver = new MultiQueueReceiver(
            receivers: [
                'emails'        => $emails,
                'notifications' => $notifications,
            ],
            strategy:   new PriorityStrategy(),
            priorities: ['emails' => 3, 'notifications' => 1],
        );

        $this->withWarningsAsErrors(static function () use ($receiver): void {
            Assert::same($receiver->receive()?->last(QueueStamp::class)?->queue, 'emails');
        });

        $receiver->close();
    }

    public function bufferedReceiveDeliversWhenOnlyTheLastOfThreeQueuesHasWork(): void
    {
        $emails        = new InMemoryTransport(); // stays empty
        $notifications = new InMemoryTransport(); // stays empty
        $laravelJobs   = new InMemoryTransport();

        $laravelJobs->send(Envelope::wrap(new PingMessage()));

        $receiver = new MultiQueueReceiver(
            receivers: [
                'emails'        => $emails,
                'notifications' => $notifications,
                'laravel_jobs'  => $laravelJobs,
            ],
            strategy:   new PriorityStrategy(),
            priorities: ['emails' => 5, 'notifications' => 3, 'laravel_jobs' => 1],
        );

        // Two queues drop out of the filtered list, so the only survivor sits at
        // key 2 and nothing occupies key 0.
        $this->withWarningsAsErrors(static function () use ($receiver): void {
            Assert::same($receiver->receive()?->last(QueueStamp::class)?->queue, 'laravel_jobs');
        });

        $receiver->close();
    }

    public function bufferedReceiveDeliversOnEqualPriorities(): void
    {
        $emails      = new InMemoryTransport(); // stays empty
        $laravelJobs = new InMemoryTransport();

        $laravelJobs->send(Envelope::wrap(new PingMessage()));

        $receiver = new MultiQueueReceiver(
            receivers: [
                'emails'       => $emails,
                'laravel_jobs' => $laravelJobs,
            ],
            strategy:   new PriorityStrategy(),
            priorities: ['emails' => 1, 'laravel_jobs' => 1],
        );

        // Equal priorities make every credit equal, so the winner comes down to
        // iteration order over a list whose first key is no longer 0.
        $this->withWarningsAsErrors(static function () use ($receiver): void {
            Assert::same($receiver->receive()?->last(QueueStamp::class)?->queue, 'laravel_jobs');
        });

        $receiver->close();
    }

    public function bufferedReceiveDeliversEveryMessageOfASurvivingQueue(): void
    {
        $emails      = new InMemoryTransport(); // stays empty
        $laravelJobs = new InMemoryTransport();

        for ($i = 0; $i < 4; $i++) {
            $laravelJobs->send(Envelope::wrap(new PingMessage()));
        }

        $receiver = new MultiQueueReceiver(
            receivers: [
                'emails'       => $emails,
                'laravel_jobs' => $laravelJobs,
            ],
            strategy:   new PriorityStrategy(),
            priorities: ['emails' => 3, 'laravel_jobs' => 1],
        );

        $this->withWarningsAsErrors(static function () use ($receiver): void {
            for ($i = 0; $i < 4; $i++) {
                Assert::same($receiver->receive()?->last(QueueStamp::class)?->queue, 'laravel_jobs');
            }
        });

        $receiver->close();
    }

    public function bufferedRoundRobinDeliversWhenOnlyALaterQueueHasWork(): void
    {
        $emails        = new InMemoryTransport(); // stays empty
        $notifications = new InMemoryTransport(); // stays empty
        $laravelJobs   = new InMemoryTransport();

        $laravelJobs->send(Envelope::wrap(new PingMessage()));

        // Default strategy, same filtered list: round-robin reindexes before it
        // reads, and must keep doing so.
        $receiver = new MultiQueueReceiver(receivers: [
            'emails'        => $emails,
            'notifications' => $notifications,
            'laravel_jobs'  => $laravelJobs,
        ]);

        $this->withWarningsAsErrors(static function () use ($receiver): void {
            Assert::same($receiver->receive()?->last(QueueStamp::class)?->queue, 'laravel_jobs');
        });

        $receiver->close();
    }

    public function returnsNullWhenAllQueuesEmpty(): void
    {
        $receiver = new MultiQueueReceiver(receivers: [
            'a' => new InMemoryTransport(),
            'b' => new InMemoryTransport(),
        ]);

        Assert::same($receiver->tryReceive(), null);

        $receiver->close();
    }
}
