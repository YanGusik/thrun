<?php

declare(strict_types=1);

namespace Thrun\Serialization;

use Thrun\Contract\MessageTypeResolverInterface;
use Thrun\Contract\NormalizableStampInterface;
use Thrun\Contract\SerializerInterface;
use Thrun\Contract\StampInterface;
use Thrun\Envelope\Envelope;
use Thrun\Envelope\Stamp\MessageIdStamp;

/**
 * JSON serializer for Envelope.
 *
 * Format:
 * {
 *   "body": {"id": 123, ...},
 *   "headers": {
 *     "type": "App\\Message\\SendEmail",
 *     "route_key": "emails",
 *     "message_id": "abc-123",
 *     "stamps": {
 *       "Thrun\\Envelope\\Stamp\\PartitionStamp": [{"key": "user1"}],
 *       "Thrun\\Envelope\\Stamp\\RetryStamp": [{"attempts": 1, "backoff": [1000, 2000]}]
 *     }
 *   }
 * }
 */
final class JsonSerializer implements SerializerInterface
{
    public function __construct(
        private readonly MessageTypeResolverInterface $typeResolver,
    ) {}

    public function extractStamps(Envelope $envelope): array
    {
        $stamps = [];
        foreach ($envelope->allStamps() as $stamp) {
            $class = $stamp::class;
            $data  = $stamp instanceof NormalizableStampInterface
                ? $stamp->normalize()
                : $this->objectToArray($stamp);

            $stamps[$class][] = $data;
        }

        return $stamps;
    }

    public function serialize(Envelope $envelope): string
    {
        $stamps    = $this->extractStamps($envelope);
        $messageId = null;

        if (isset($stamps[MessageIdStamp::class])) {
            $messageId = $stamps[MessageIdStamp::class][0]['id'] ?? null;
            unset($stamps[MessageIdStamp::class]);
        }

        $payload = [
            'body'    => json_decode(json_encode($envelope->message), true),
            'headers' => [
                'type'       => $envelope->type ?? (is_object($envelope->message) ? $envelope->message::class : 'array'),
                'route_key'  => $envelope->routeKey,
                'message_id' => $messageId,
                'stamps'     => $stamps,
            ],
        ];

        return json_encode($payload, JSON_THROW_ON_ERROR);
    }

    public function deserialize(string $data): Envelope
    {
        $decoded = json_decode($data, true, 512, JSON_THROW_ON_ERROR);

        if (!isset($decoded['headers']['type'])) {
            throw new \RuntimeException('Missing message type in envelope headers');
        }

        $type     = $decoded['headers']['type'];
        $routeKey = $decoded['headers']['route_key'] ?? null;

        $resolvedClass = $this->typeResolver->resolve($type);
        if (class_exists($resolvedClass) && $resolvedClass !== 'stdClass') {
            $message = $this->instantiate($resolvedClass, $decoded['body'] ?? []);
        } else {
            $message = (array) ($decoded['body'] ?? []);
        }

        $stamps = [];
        foreach ($decoded['headers']['stamps'] ?? [] as $stampClass => $stampList) {
            $resolvedStampClass = $this->typeResolver->resolve($stampClass);
            foreach ((array) $stampList as $stampData) {
                $stamps[] = $this->deserializeStamp($resolvedStampClass, (array) $stampData);
            }
        }

        if (isset($decoded['headers']['message_id'])) {
            $stamps[] = new MessageIdStamp($decoded['headers']['message_id']);
        }

        return new Envelope($message, type: $type, routeKey: $routeKey, stamps: $stamps);
    }

    private function objectToArray(object $object): array
    {
        return json_decode(json_encode($object), true);
    }

    /**
     * @param class-string $class
     */
    private function deserializeStamp(string $class, array $data): StampInterface
    {
        if (is_a($class, NormalizableStampInterface::class, true)) {
            return $class::denormalize($data);
        }

        return $this->instantiate($class, $data);
    }

    /**
     * @param class-string $class
     * @param array<string, mixed> $data
     */
    private function instantiate(string $class, array $data): object
    {
        $rc   = new \ReflectionClass($class);
        $ctor = $rc->getConstructor();

        if ($ctor === null) {
            return $rc->newInstanceWithoutConstructor();
        }

        $args = [];
        foreach ($ctor->getParameters() as $param) {
            $name  = $param->getName();
            $value = $data[$name] ?? null;

            $type = $param->getType();
            if ($type instanceof \ReflectionNamedType && !$type->isBuiltin()) {
                $typeName = $type->getName();
                if (is_a($typeName, \BackedEnum::class, true) && (is_string($value) || is_int($value))) {
                    $value = $typeName::from($value);
                } elseif (class_exists($typeName) && is_array($value)) {
                    $value = $this->instantiate($typeName, $value);
                }
            } elseif ($type instanceof \ReflectionNamedType && $type->isBuiltin()) {
                $value = match ($type->getName()) {
                    'int'    => (int) $value,
                    'float'  => (float) $value,
                    'bool'   => (bool) $value,
                    'string' => (string) $value,
                    default  => $value,
                };
            }

            $args[] = $value;
        }

        return $rc->newInstanceArgs($args);
    }
}
