<?php

namespace karmabunny\rdb\Objects;

use InvalidArgumentException;
use JsonSerializable;
use karmabunny\rdb\JsonDeserializable;
use karmabunny\rdb\Rdb;
use karmabunny\rdb\RdbObjectDriver;

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

        $ok = $this->rdb->setHash($key, $value);
        if (!$ok) return false;

        if ($ttl > 0) {
            $ok = $this->rdb->expire($key, $ttl);
            if (!$ok) return false;
        }

        return true;
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

            $ok = $this->rdb->setHash($key, $item);
            $sizes[$key] = $ok ? count($item) : 0;
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