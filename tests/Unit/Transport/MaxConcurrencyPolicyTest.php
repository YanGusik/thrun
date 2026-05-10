<?php

declare(strict_types=1);

namespace Thrun\Tests\Unit\Transport;

use Testo\Assert;
use Thrun\Envelope\Envelope;
use Thrun\Envelope\Stamp\PartitionStamp;
use Thrun\Tests\AsyncTestCase;
use Thrun\Tests\Fixture\PingMessage;
use Thrun\Transport\Policy\MaxConcurrencyPolicy;
use Thrun\Transport\Policy\StaticConcurrencyResolver;

final class MaxConcurrencyPolicyTest extends AsyncTestCase
{
    public function allowsWhenBelowLimit(): void
    {
        $policy = new MaxConcurrencyPolicy(maxPerPartition: 2);
        $envelope = Envelope::wrap(new PingMessage())->with(new PartitionStamp('user1'));

        Assert::same($policy->allows($envelope), true);
    }

    public function deniesWhenAtLimit(): void
    {
        $policy = new MaxConcurrencyPolicy(maxPerPartition: 1);
        $envelope = Envelope::wrap(new PingMessage())->with(new PartitionStamp('user1'));

        $policy->acquire($envelope);

        Assert::same($policy->allows($envelope), false);
    }

    public function differentPartitionsAreIndependent(): void
    {
        $policy = new MaxConcurrencyPolicy(maxPerPartition: 1);

        $user1 = Envelope::wrap(new PingMessage())->with(new PartitionStamp('user1'));
        $user2 = Envelope::wrap(new PingMessage())->with(new PartitionStamp('user2'));

        $policy->acquire($user1);

        Assert::same($policy->allows($user2), true);
    }

    public function releaseFreesSlot(): void
    {
        $policy = new MaxConcurrencyPolicy(maxPerPartition: 1);
        $envelope = Envelope::wrap(new PingMessage())->with(new PartitionStamp('user1'));

        $policy->acquire($envelope);
        Assert::same($policy->allows($envelope), false);

        $policy->release($envelope);
        Assert::same($policy->allows($envelope), true);
    }

    public function envelopeWithoutPartitionStampUsesDefaultPartition(): void
    {
        $policy = new MaxConcurrencyPolicy(maxPerPartition: 1);

        $first  = Envelope::wrap(new PingMessage());
        $second = Envelope::wrap(new PingMessage());

        // No PartitionStamp → falls back to 'default' partition
        Assert::same($policy->allows($first), true);
        $policy->acquire($first);

        // 'default' partition at limit
        Assert::same($policy->allows($second), false);
    }

    public function ignorePartitionBypassesLimit(): void
    {
        $policy = new MaxConcurrencyPolicy(maxPerPartition: 1);

        $ignored = Envelope::wrap(new PingMessage())->with(new PartitionStamp(StaticConcurrencyResolver::IGNORE));

        // Should always allow, even after acquire
        Assert::same($policy->allows($ignored), true);
        $policy->acquire($ignored);
        Assert::same($policy->allows($ignored), true);
        $policy->acquire($ignored);
        Assert::same($policy->allows($ignored), true);
    }

    public function ignoredPartitionBypassesLimit(): void
    {
        $policy = new MaxConcurrencyPolicy(maxPerPartition: 1);

        $ignored = Envelope::wrap(new PingMessage())->with(new PartitionStamp(StaticConcurrencyResolver::IGNORED));

        Assert::same($policy->allows($ignored), true);
        $policy->acquire($ignored);
        Assert::same($policy->allows($ignored), true);
    }

    public function ignoredPartitionDoesNotAffectOthers(): void
    {
        $policy = new MaxConcurrencyPolicy(maxPerPartition: 1);

        $user1   = Envelope::wrap(new PingMessage())->with(new PartitionStamp('user1'));
        $ignored = Envelope::wrap(new PingMessage())->with(new PartitionStamp(StaticConcurrencyResolver::IGNORE));

        $policy->acquire($user1);

        // user1 is at limit, but ignored should still be allowed
        Assert::same($policy->allows($ignored), true);
        // user1 should still be denied
        Assert::same($policy->allows($user1), false);
    }
}
