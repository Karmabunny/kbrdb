<?php
declare(strict_types=1);

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
    protected Rdb $rdb;


    public function __construct(Rdb $rdb)
    {
        $this->rdb = $rdb;
    }


    /** @inheritdoc */
    public function inspect(string $key): ?string
    {
        $value = $this->rdb->get($key);

        if ($value === null) {
            return null;
        }

        if (!preg_match('/^O:\d+:"([^"]+)"/', $value, $matches)) {
            return null;
        }

        [, $class] = $matches;
        return $class;
    }


    /** @inheritdoc */
    public function setObject(string $key, object $value, int|float $ttl = 0): int
    {
        $value = serialize($value);
        if (!$this->rdb->set($key, $value, $ttl)) return 0;
        return strlen($value);
    }


    /** @inheritdoc */
    public function getObject(string $key, string $expected): ?object
    {
        $value = $this->rdb->get($key);
        if ($value === null) return null;

        $value = @unserialize($value, ['allowed_classes' => [$expected]]);

        if ($value === false) return null;
        if (!is_object($value)) return null;
        if ($expected !== get_class($value)) return null;

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
    public function mGetObjects(array $keys, string $expected): array
    {
        $items = $this->rdb->mGet($keys);

        $output = [];

        foreach ($items as $key => $item) {
            if (!$key or !$item) continue;

            $item = @unserialize($item, ['allowed_classes' => [$expected]]);

            if (!$item) continue;
            if (!is_object($item)) continue;
            if ($expected !== get_class($item)) continue;

            $output[$key] = $item;
        }

        return $output;
    }
}
