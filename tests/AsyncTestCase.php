<?php

declare(strict_types=1);

namespace Thrun\Tests;

use Async\Scope;
use Testo\Assert;
use Testo\Lifecycle\AfterTest;
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
    #[AfterTest]
    public function assertNoZombieCoroutines(): void
    {
        delay(50);
        $coroutines = get_coroutines();
        if (count($coroutines) !== 1) {
            foreach ($coroutines as $coro) {
                if ($coro === current_coroutine()) {
                    continue;
                }
//                error_log("\n[ZOMBIE] spawn: ".$coro->getSpawnLocation());
//                error_log("[ZOMBIE] suspended: ".($coro->getSuspendLocation() ?: 'not suspended'));
//                error_log("[ZOMBIE] isSuspended: ".($coro->isSuspended() ? 'yes' : 'no'));
//                error_log("[ZOMBIE] isCompleted: ".($coro->isCompleted() ? 'yes' : 'no'));
//                error_log("[ZOMBIE] isCancelled: ".($coro->isCancelled() ? 'yes' : 'no'));
            }
        }

        if (count($coroutines) !== 1)
        {
            Assert::fail("zombie coroutines are expected");
        }

//        Assert::same(count($coroutines), 1, 'zombie coroutines are expected');
    }

    /**
     * Runs the callback with everything PHP routes through the error handler —
     * warning, notice, deprecation — rethrown as an ErrorException, the way a
     * framework error handler does. The exception reaches the caller; the
     * previous handler is restored either way.
     *
     * A test prints a warning and walks past it; an application under such a
     * handler loses the coroutine that raised it.
     */
    protected function withWarningsAsErrors(callable $callback): void
    {
        set_error_handler(static function (int $severity, string $message, string $file, int $line): bool {
            throw new \ErrorException($message, 0, $severity, $file, $line);
        });

        try {
            $callback();
        } finally {
            restore_error_handler();
        }
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
