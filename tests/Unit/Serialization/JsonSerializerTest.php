<?php

declare(strict_types=1);

namespace Thrun\Tests\Unit\Serialization;

use Testo\Assert;
use Thrun\Envelope\Stamp\JobIdStamp;
use Thrun\Serialization\ClassMapMessageTypeResolver;
use Thrun\Serialization\JsonSerializer;
use Thrun\Envelope\Envelope;
use Thrun\Envelope\Stamp\PartitionStamp;
use Thrun\Tests\AsyncTestCase;
use Thrun\Tests\Fixture\PingMessage;
use Thrun\Tests\Fixture\SendEmailMessage;

final class JsonSerializerTest extends AsyncTestCase
{
    public function serializeDeserializeRoundtrip(): void
    {
        $resolver   = new ClassMapMessageTypeResolver();
        $serializer = new JsonSerializer($resolver);
        $envelope   = Envelope::wrap(new SendEmailMessage('to@example.com', 'Hello'));

        $json       = $serializer->serialize($envelope);
        $restored   = $serializer->deserialize($json);

        Assert::same($restored->message::class, SendEmailMessage::class);
        Assert::same($restored->message->to, 'to@example.com');
        Assert::same($restored->message->subject, 'Hello');
    }

    public function classMapAliasResolution(): void
    {
        $resolver = new ClassMapMessageTypeResolver();
        $resolver->register('ping', PingMessage::class);

        $serializer = new JsonSerializer($resolver);
        $json = json_encode([
            'body'    => new \stdClass(),
            'headers' => [
                'type'   => 'ping',
                'stamps' => new \stdClass(),
            ],
        ]);

        $restored = $serializer->deserialize($json);

        Assert::same($restored->message::class, PingMessage::class);
    }

    public function emptyMessageWithJobIdStamp(): void
    {
        $resolver   = new ClassMapMessageTypeResolver();
        $serializer = new JsonSerializer($resolver);
        $envelope   = Envelope::wrap(new PingMessage());

        $json     = $serializer->serialize($envelope);
        $restored = $serializer->deserialize($json);

        Assert::same(count($restored->allStamps()), 1);
        Assert::notNull($envelope->last(JobIdStamp::class)?->id);
    }

    public function invalidJsonThrowsException(): void
    {
        $resolver   = new ClassMapMessageTypeResolver();
        $serializer = new JsonSerializer($resolver);

        try {
            $serializer->deserialize('not json at all');
            Assert::fail('Expected JsonException was not thrown');
        } catch (\JsonException $e) {
            Assert::same(true, true);
        }
    }

    public function missingTypeThrowsException(): void
    {
        $resolver   = new ClassMapMessageTypeResolver();
        $serializer = new JsonSerializer($resolver);
        $json = json_encode([
            'body'    => new \stdClass(),
            'headers' => [
                'stamps' => new \stdClass(),
            ],
        ]);

        try {
            $serializer->deserialize($json);
            Assert::fail('Expected RuntimeException was not thrown');
        } catch (\RuntimeException $e) {
            Assert::same($e->getMessage(), 'Missing message type in envelope headers');
        }
    }

    public function stampsAreRestored(): void
    {
        $resolver   = new ClassMapMessageTypeResolver();
        $serializer = new JsonSerializer($resolver);
        $envelope   = Envelope::wrap(new PingMessage())
            ->with(new PartitionStamp('user1'));

        $json     = $serializer->serialize($envelope);
        $restored = $serializer->deserialize($json);

        Assert::same($restored->last(PartitionStamp::class)?->key, 'user1');
    }
}
