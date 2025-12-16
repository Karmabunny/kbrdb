<?php

namespace karmabunny\rdb\Objects;

use InvalidArgumentException;
use JsonSerializable;
use karmabunny\interfaces\JsonDeserializable;
use karmabunny\rdb\Rdb;
use karmabunny\rdb\RdbObjectDriver;

/**
 * Store an object as a flattened hash.
 *
 * This converts nested object + arrays into a flattened form using a dot notation.
 *
 * For example:
 *
 * ```
 * ['abc' => [
 *     'def' => [123, 456]
 * ]]
 * ```
 *
 * becomes:
 *
 * ```
 * 'abc.def.0' => 123
 * 'abc.def.1' => 456
 * ```
 *
 * This somewhat breaks the setObject() signature, where the return value is
 * the number of keys rather than the number of bytes.
 *
 * Objects are expected to implement `JsonSerializable` and `JsonDeserializable`.
 * For convenience, implement the `RdbJsonObject` interface.
 *
 * The 'expected' parameter is required.
 *
 * @package karmabunny\rdb\Objects
 */
class HashObjectDriver implements RdbObjectDriver
{

    /**
     * @var Rdb
     */
    protected $rdb;


    public function __construct(Rdb $rdb)
    {
        $this->rdb = $rdb;
    }


    /** @inheritdoc */
    public function setObject(string $key, object $value, $ttl = 0): int
    {
        if (!$value instanceof JsonSerializable) {
            throw new InvalidArgumentException('Object must implement JsonSerializable');
        }

        $value = $value->jsonSerialize();

        if (!is_array($value)) {
            throw new InvalidArgumentException('Object must serialize to an array');
        }

        $count = $this->rdb->setHash($key, $value);

        if ($count and $ttl > 0) {
            $this->rdb->expire($key, $ttl);
        }

        return $count;
    }


    /** @inheritdoc */
    public function getObject(string $key, ?string $expected = null): ?object
    {
        if ($expected === null) {
            throw new InvalidArgumentException('Expected class is required');
        }

        if (!is_subclass_of($expected, JsonDeserializable::class)) {
            throw new InvalidArgumentException("Expected class must implement JsonDeserializable: {$expected}");
        }

        /** @var JsonDeserializable $expected */

        $value = $this->rdb->getHash($key);
        if ($value === null) {
            return null;
        }

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

            $item = $item->jsonSerialize();

            if (!is_array($item)) {
                throw new InvalidArgumentException('Object must serialize to an array');
            }

            $sizes[$key] = $this->rdb->setHash($key, $item);
        }

        return $sizes;
    }


    /** @inheritdoc */
    public function mGetObjects(array $keys, ?string $expected = null): array
    {
        if ($expected === null) {
            throw new InvalidArgumentException('Expected class is required');
        }

        if (!is_subclass_of($expected, JsonDeserializable::class)) {
            throw new InvalidArgumentException("Expected class must implement JsonSerializable: {$expected}");
        }

        /** @var JsonDeserializable $expected */

        $output = [];

        foreach ($keys as $key) {
            $item = $this->rdb->getHash($key);

            if ($item === null) {
                continue;
            }

            $output[$key] = $expected::fromJson($item);
        }

        return $output;
    }
}