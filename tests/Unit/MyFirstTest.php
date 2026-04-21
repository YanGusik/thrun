<?php

namespace Thrun\Tests\Unit;

use Testo\Assert;
use Testo\Test;
use function Async\await;
use function Async\spawn;

#[Test]
final class MyFirstTest
{
    public function checkSpawn(): void
    {
        $sp = spawn(function () {
           return 'hello world';
        });

        $result = await($sp);

        Assert::same($result, 'hello world');
    }

    public function dividesNumbers(): void
    {
        $result = 10 / 2;

        Assert::same($result, 5);
        Assert::notSame($result, 5.0); // Types matter
    }
}