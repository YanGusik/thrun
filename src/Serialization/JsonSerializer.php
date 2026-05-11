<?php

declare(strict_types=1);

namespace Thrun\Serialization;

use Thrun\Contract\MessageTypeResolverInterface;
use Thrun\Contract\SerializerInterface;
use Thrun\Contract\StampInterface;
use Thrun\Envelope\Envelope;

/**
 * JSON serializer for Envelope.
 *
 * Format:
 * {
 *   "body": {"id": 123, ...},
 *   "headers": {
 *     "type": "App\\Message\\SendEmail",
 *     "stamps": {
 *       "Thrun\\Envelope\\Stamp\\PartitionStamp": {"key": "user1"}
 *     }
 *   }
 * }
 */
final class JsonSerializer implements SerializerInterface
{
    public function __construct(
        private readonly MessageTypeResolverInterface $typeResolver,
    ) {}

    public function serialize(Envelope $envelope): string
    {
        $stamps = [];
        foreach ($envelope->allStamps() as $stamp) {
            $stamps[$stamp::class] = $stamp;
        }

        $payload = [
            'body'    => json_decode(json_encode($envelope->message), true),
            'headers' => [
                'type'   => $envelope->message::class,
                'stamps' => $stamps,
            ],
        ];

        return json_encode($payload, JSON_THROW_ON_ERROR);
    }

    public function deserialize(string $data): Envelope
    {
        $decoded = json_decode($data, false, 512, JSON_THROW_ON_ERROR);

        if (!isset($decoded->headers->type)) {
            throw new \RuntimeException('Missing message type in envelope headers');
        }

        $messageClass = $this->typeResolver->resolve($decoded->headers->type);
        $message      = $this->instantiate($messageClass, $decoded->body);

        $stamps = [];
        foreach ($decoded->headers->stamps ?? new \stdClass() as $stampClass => $stampData) {
            $resolvedClass = $this->typeResolver->resolve($stampClass);
            $stamps[]      = $this->instantiate($resolvedClass, $stampData);
        }

        return new Envelope($message, ...$stamps);
    }

    private function instantiate(string $class, \stdClass|array|null $data): object
    {
        if ($data === null) {
            $data = new \stdClass();
        }
        if (is_array($data)) {
            $data = (object) $data;
        }

        $rc   = new \ReflectionClass($class);
        $ctor = $rc->getConstructor();

        if ($ctor === null) {
            return $rc->newInstanceWithoutConstructor();
        }

        $args = [];
        foreach ($ctor->getParameters() as $param) {
            $name  = $param->getName();
            $value = $data->$name ?? null;

            $type = $param->getType();
            if ($type instanceof \ReflectionNamedType && !$type->isBuiltin()) {
                $typeName = $type->getName();
                if (is_a($typeName, \BackedEnum::class, true) && is_string($value)) {
                    $value = $typeName::from($value);
                } elseif (class_exists($typeName) && $value instanceof \stdClass) {
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
