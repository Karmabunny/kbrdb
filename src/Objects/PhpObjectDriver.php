<?php

namespace karmabunny\rdb\Objects;

use karmabunny\rdb\Rdb;
use karmabunny\rdb\RdbObjectDriver;

/**
 * Store an object as a PHP serialized string.
 *
 * This doesn't require that objects implement any particular interface.
 *
 * This is the default object driver and the the simplest.
 *
 * @package karmabunny\rdb\Objects
 */
class PhpObjectDriver implements RdbObjectDriver
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
        $value = serialize($value);
        if (!$this->rdb->set($key, $value, $ttl)) return 0;
        return strlen($value);
    }


    /** @inheritdoc */
    public function getObject(string $key, ?string $expected = null): ?object
    {
        $value = $this->rdb->get($key);
        $value = @unserialize($value);

        if ($value === false) return null;
        if (!is_object($value)) return null;
        if ($expected and !is_a($value, $expected, false)) return null;

        return $value;
    }


    /** @inheritdoc */
    public function mSetObjects(iterable $items): array
    {
        $sizes = [];

        /** @var string[] $items */
        foreach ($items as $key => &$item) {
            $item = serialize($item);
            $sizes[$key] = strlen($item);
        }
        unset($item);

        $ok = $this->rdb->mSet($items);

        if (!$ok) {
            return [];
        }

        return $sizes;
    }


    /** @inheritdoc */
    public function mGetObjects(array $keys, ?string $expected = null): array
    {
        $items = $this->rdb->mGet($keys);

        $output = [];

        foreach ($items as $key => $item) {
            $item = @unserialize($item) ?: null;

            if (!$key or !$item) continue;

            if (!is_object($item)) continue;

            if ($expected and !is_a($item, $expected, false)) continue;

            $output[$key] = $item;
        }

        return $output;
    }
}
