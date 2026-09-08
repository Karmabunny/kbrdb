<?php
declare(strict_types=1);

namespace karmabunny\rdb\Objects;

use Exception;
use InvalidArgumentException;
use JsonSerializable;
use karmabunny\interfaces\JsonDeserializable;
use karmabunny\rdb\Rdb;
use karmabunny\rdb\RdbObjectDriver;
use MessagePack\Exception\UnpackingFailedException;
use MessagePack\MessagePack;

/**
 * Store an object as a MessagePack string.
 *
 * Objects are expected to implement `JsonSerializable` and `JsonDeserializable`.
 * For convenience, implement the `RdbJsonObject` interface.
 *
 * The 'expected' parameter is required.
 *
 * @package karmabunny\rdb\Objects
 */
class MsgPackObjectDriver implements RdbObjectDriver
{

    /**
     * @var Rdb
     */
    protected Rdb $rdb;


    public function __construct(Rdb $rdb)
    {
        $this->rdb = $rdb;
    }


    /** @inheritdoc */
    public function inspect(string $key): ?string
    {
        $value = $this->rdb->unpack($key);

        if ($value === null or !is_array($value)) {
            return null;
        }

        return $value['__class__'] ?? null;
    }


    /** @inheritdoc */
    public function setObject(string $key, object $value, int|float $ttl = 0): int
    {
        if (!$value instanceof JsonSerializable) {
            throw new InvalidArgumentException('Object must implement JsonSerializable');
        }

        $blob = $value->jsonSerialize();

        if (!is_array($blob)) {
            throw new InvalidArgumentException('Object must serialize to an array');
        }

        $blob['__class__'] = get_class($value);
        return $this->rdb->pack($key, $blob, $ttl);
    }


    /** @inheritdoc */
    public function getObject(string $key, string $expected): ?object
    {
        if (!is_subclass_of($expected, JsonDeserializable::class)) {
            throw new InvalidArgumentException("Expected class must implement JsonDeserializable: {$expected}");
        }

        /** @var class-string<JsonDeserializable> $expected */

        try {
            $value = $this->rdb->unpack($key);
            if ($value === null) {
                return null;
            }
        }
        catch (UnpackingFailedException $_error) {
            return null;
        }

        if (isset($value['__class__']) and $value['__class__'] !== $expected) {
            return null;
        }

        unset($value['__class__']);
        return $expected::fromJson($value);
    }


    /** @inheritdoc */
    public function mSetObjects(iterable $items): array
    {
        $sizes = [];

        foreach ($items as $key => &$item) {
            if (!$item instanceof JsonSerializable) {
                unset($items[$key]);
                continue;
            }

            $blob = $item->jsonSerialize();

            if (!is_array($blob)) {
                throw new InvalidArgumentException('Object must serialize to an array');
            }

            $blob['__class__'] = get_class($item);
            $blob = MessagePack::pack($blob);

            $sizes[$key] = strlen($blob);
            $item = $blob;
        }
        unset($item);

        $ok = $this->rdb->mSet($items);

        if (!$ok) {
            return [];
        }

        return $sizes;
    }


    /** @inheritdoc */
    public function mGetObjects(array $keys, string $expected): array
    {
        if (!is_subclass_of($expected, JsonDeserializable::class)) {
            throw new InvalidArgumentException("Expected class must implement JsonSerializable: {$expected}");
        }

        /** @var class-string<JsonDeserializable> $expected */

        $items = $this->rdb->mGet($keys);
        $output = [];

        foreach ($items as $key => $item) {
            if (!$key or !$item) {
                continue;
            }

            // Don't raise errors here, just skip.
            try {
                $value = MessagePack::unpack($item);
            }
            catch (Exception $e) {
                continue;
            }

            if (!is_array($value)) {
                continue;
            }

            if (isset($value['__class__']) and $value['__class__'] !== $expected) {
                continue;
            }

            unset($value['__class__']);
            $output[$key] = $expected::fromJson($value);
        }

        return $output;
    }
}