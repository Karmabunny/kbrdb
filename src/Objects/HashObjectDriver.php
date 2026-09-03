<?php
declare(strict_types=1);

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
    protected Rdb $rdb;


    public function __construct(Rdb $rdb)
    {
        $this->rdb = $rdb;
    }


    /** @inheritdoc */
    public function setObject(string $key, object $value, int $ttl = 0): int
    {
        if (!$value instanceof JsonSerializable) {
            throw new InvalidArgumentException('Object must implement JsonSerializable');
        }

        $blob = $value->jsonSerialize();

        if (!is_array($blob)) {
            throw new InvalidArgumentException('Object must serialize to an array');
        }

        $blob['__class__'] = get_class($value);

        $count = $this->rdb->setHash($key, $blob);

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
            $sizes[$key] = $this->rdb->setHash($key, $blob);
        }
        unset($item);

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

            if (isset($item['__class__']) and $item['__class__'] !== $expected) {
                continue;
            }

            unset($item['__class__']);
            $output[$key] = $expected::fromJson($item);
        }

        return $output;
    }
}