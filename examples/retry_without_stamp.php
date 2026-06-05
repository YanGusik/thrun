<?php

declare(strict_types=1);

require_once __DIR__.'/../vendor/autoload.php';

use Thrun\Envelope\Envelope;
use Thrun\Envelope\Stamp\MessageIdStamp;
use Thrun\Envelope\Stamp\RedeliveryStamp;
use Thrun\Middleware\CatchMessageMiddleware;
use Thrun\Supervisor\Supervisor;
use Thrun\Supervisor\SupervisorOptions;
use Thrun\Tests\Fixture\PingMessage;
use Thrun\Transport\InMemory\InMemoryTransport;
use Thrun\Worker\Acknowledger;
use Thrun\Worker\Worker;
use Thrun\Worker\WorkerOptions;

$emails = new InMemoryTransport();

$emails->send(Envelope::wrap(new PingMessage(), new MessageIdStamp('one')));

$supervisor = new Supervisor(
    workerFactory: fn() => new Worker(
        transport: $emails,
        handlers: [
            PingMessage::class => function (PingMessage $m, Acknowledger $ack) {
                $redelivery = $ack->envelope->last(RedeliveryStamp::class);
                $firstRedelivery = array_first($ack->envelope->all(RedeliveryStamp::class));

                if ($redelivery) {
                    echo "job retry ({$redelivery->attempt},{$redelivery->retriedAt})(first retry: {$firstRedelivery->attempt},{$firstRedelivery->retriedAt})\n";
                }
                else {
                    echo "job retry\n";
                }

                $ack->retry(1000);
            },
        ],
        options: new WorkerOptions(threads: 1, concurrency: 1, middleware: [new CatchMessageMiddleware()]),
    ),
    options: new SupervisorOptions(),
);

$supervisor->run();

/**
 * Expected:
 * job retry
 * job retry (1,2026-06-05T11:39:44+00:00)(first retry: 1,2026-06-05T11:39:44+00:00)
 * job retry (2,2026-06-05T11:39:45+00:00)(first retry: 1,2026-06-05T11:39:44+00:00)
 * job retry (3,2026-06-05T11:39:46+00:00)(first retry: 1,2026-06-05T11:39:44+00:00)
 * job retry (4,2026-06-05T11:39:47+00:00)(first retry: 1,2026-06-05T11:39:44+00:00)
 * job retry (5,2026-06-05T11:39:48+00:00)(first retry: 1,2026-06-05T11:39:44+00:00)
 * job retry (6,2026-06-05T11:39:49+00:00)(first retry: 1,2026-06-05T11:39:44+00:00)
 * job retry (7,2026-06-05T11:39:50+00:00)(first retry: 1,2026-06-05T11:39:44+00:00)
 * job retry (8,2026-06-05T11:39:51+00:00)(first retry: 1,2026-06-05T11:39:44+00:00)
 * job retry (9,2026-06-05T11:39:52+00:00)(first retry: 1,2026-06-05T11:39:44+00:00)
 * job retry (10,2026-06-05T11:39:53+00:00)(first retry: 1,2026-06-05T11:39:44+00:00)
 * job retry (11,2026-06-05T11:39:54+00:00)(first retry: 1,2026-06-05T11:39:44+00:00)
 * job retry (12,2026-06-05T11:39:55+00:00)(first retry: 1,2026-06-05T11:39:44+00:00)
 * ^C
 * [Supervisor] Signal SIGINT received. Stopping worker...
 */