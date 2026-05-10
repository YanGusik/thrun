<?php

declare(strict_types=1);

namespace Thrun\Tests;

use Testo\Assert;
use Testo\Lifecycle\BeforeTest;
use Testo\Test;
use function Async\current_coroutine;
use function Async\delay;
use function Async\get_coroutines;

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
                echo "\n[ZOMBIE] spawn: " . $coro->getSpawnLocation() . "\n";
                echo "[ZOMBIE] suspended: " . ($coro->getSuspendLocation() ?: 'not suspended') . "\n";
                echo "[ZOMBIE] isSuspended: " . ($coro->isSuspended() ? 'yes' : 'no') . "\n";
                echo "[ZOMBIE] isCompleted: " . ($coro->isCompleted() ? 'yes' : 'no') . "\n";
                echo "[ZOMBIE] isCancelled: " . ($coro->isCancelled() ? 'yes' : 'no') . "\n";
            }
        }
        Assert::same(count($coroutines), 1);
    }
}
