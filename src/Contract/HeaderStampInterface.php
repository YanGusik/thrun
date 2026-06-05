<?php

declare(strict_types=1);

namespace Thrun\Contract;

/**
 * Marker interface for stamps that are serialized as top-level envelope headers
 * rather than nested inside headers.stamps.
 *
 * This is an exception to the normal stamp serialization flow. The serializer
 * must handle each HeaderStampInterface implementation explicitly.
 */
interface HeaderStampInterface extends StampInterface
{
}
