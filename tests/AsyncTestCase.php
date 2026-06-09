<?php

declare(strict_types=1);

namespace Thrun\Tests;

use Async\Scope;
use Testo\Assert;
use Testo\Lifecycle\BeforeTest;
use Testo\Test;
use Thrun\Transport\InMemory\InMemoryTransport;
use Thrun\Worker\Worker;
use function Async\current_coroutine;
use function Async\delay;
use function Async\get_coroutines;
use function Async\spawn;

#[Test]
abstract class AsyncTestCase
{
    #[BeforeTest]
    public function assertNoZombieCoroutines(): void
    {
        delay(50);
        $coroutines = get_coroutines();
        if (count($coroutines) !== 1) {
            foreach ($coroutines as $coro) {
                if ($coro === current_coroutine()) {
                    continue;
                }
                echo "\n[ZOMBIE] spawn: ".$coro->getSpawnLocation()."\n";
                echo "[ZOMBIE] suspended: ".($coro->getSuspendLocation() ?: 'not suspended')."\n";
                echo "[ZOMBIE] isSuspended: ".($coro->isSuspended() ? 'yes' : 'no')."\n";
                echo "[ZOMBIE] isCompleted: ".($coro->isCompleted() ? 'yes' : 'no')."\n";
                echo "[ZOMBIE] isCancelled: ".($coro->isCancelled() ? 'yes' : 'no')."\n";
            }
        }
        Assert::same(count($coroutines), 1);
    }

    protected function runWorkerAndWait(
        Worker $worker, InMemoryTransport $transport,
        ?int $expectedAcked = null, ?int $expectedRejected = null,
        int $timeoutMs = 5000
    ): void {
        $coro = spawn(function () use ($worker): void {
            $worker->run();
        });

        $scope = new Scope();
        $scope->spawn(function () use ($worker, $timeoutMs) {
            delay($timeoutMs);
            $worker->stop();
        });
        $scope->spawn(function () use ($transport, $worker, $expectedAcked, $expectedRejected) {
            while (true) {
                $ackedReached = $expectedAcked !== null
                    && $transport->ackedCount >= $expectedAcked;

                $rejectedReached = $expectedRejected !== null
                    && $transport->rejectedCount >= $expectedRejected;

                $shouldStop =
                    ($expectedAcked !== null && $expectedRejected !== null && $ackedReached && $rejectedReached)
                    || ($expectedAcked !== null && $expectedRejected === null && $ackedReached)
                    || ($expectedAcked === null && $expectedRejected !== null && $rejectedReached)
                    || ($worker->isIdle() && ($expectedAcked === null && $expectedRejected === null));

                if ($shouldStop) {
                    $worker->stop();
                    break;
                }

                delay(1);
            }
        });


        \Async\await($coro);
        $scope->cancel();
    }
}
