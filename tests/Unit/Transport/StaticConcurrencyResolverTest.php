<?php

declare(strict_types=1);

namespace Thrun\Tests\Unit\Transport;

use Testo\Assert;
use Thrun\Envelope\Envelope;
use Thrun\Envelope\Stamp\PartitionStamp;
use Thrun\Tests\AsyncTestCase;
use Thrun\Tests\Fixture\PingMessage;
use Thrun\Transport\Policy\StaticConcurrencyResolver;

final class StaticConcurrencyResolverTest extends AsyncTestCase
{
    public function returnsLimitForRegularPartition(): void
    {
        $resolver = new StaticConcurrencyResolver(limit: 5);
        $envelope = Envelope::wrap(new PingMessage())->with(new PartitionStamp('user1'));

        Assert::same($resolver->resolve('user1', $envelope), 5);
    }

    public function returnsNullForIgnore(): void
    {
        $resolver = new StaticConcurrencyResolver(limit: 5);
        $envelope = Envelope::wrap(new PingMessage())->with(new \Thrun\Envelope\Stamp\PartitionStamp(StaticConcurrencyResolver::IGNORE));

        Assert::same($resolver->resolve(StaticConcurrencyResolver::IGNORE, $envelope), null);
    }

    public function returnsNullForIgnored(): void
    {
        $resolver = new StaticConcurrencyResolver(limit: 5);
        $envelope = Envelope::wrap(new PingMessage())->with(new \Thrun\Envelope\Stamp\PartitionStamp(StaticConcurrencyResolver::IGNORED));

        Assert::same($resolver->resolve(StaticConcurrencyResolver::IGNORED, $envelope), null);
    }

    public function returnsLimitForDefaultPartition(): void
    {
        $resolver = new StaticConcurrencyResolver(limit: 3);
        $envelope = Envelope::wrap(new PingMessage());

        Assert::same($resolver->resolve('default', $envelope), 3);
    }
}
