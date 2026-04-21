<?php

declare(strict_types=1);

namespace Thrun\Tests;

use Testo\Assert;
use Testo\Lifecycle\BeforeTest;
use Testo\Test;
use function Async\get_coroutines;

#[Test]
abstract class AsyncTestCase
{
    #[BeforeTest]
    public function assertNoZombieCoroutines(): void
    {
        Assert::same(count(get_coroutines()), 1);
    }
}
