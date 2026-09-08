<?php
declare(strict_types=1);

namespace karmabunny\rdb;


/**
 * Base interface for object drivers.
 *
 * These implement different methods for serialising object data.
 *
 * The `$expected` parameter will become mandatory in v3.
 *
 * @package karmabunny\rdb
 */
interface RdbObjectDriver
{

    /**
     * Inspect the object stored in the key.
     *
     * @param string $key
     * @return string|null the object class name
     */
    public function inspect(string $key): ?string;


    /**
     * Set an object.
     *
     * @param string $key
     * @param object $value
     * @param int $ttl milliseconds
     * @return int object size in bytes
     */
    public function setObject(string $key, object $value, int $ttl = 0): int;


    /**
     * Get an object from a key.
     *
     * @template T of object
     * @param string $key
     * @param class-string<T>|null $expected
     * @return T|null
     */
    public function getObject(string $key, ?string $expected = null): ?object;


    /**
     * Set multiple objects.
     *
     * @param iterable $items [ key => object ]
     * @return int[] [ key => int ] object sizes in bytes
     */
    public function mSetObjects(iterable $items): array;


    /**
     * Get multiple objects from a key.
     *
     * @template T of object
     * @param string[] $keys
     * @param class-string<T>|null $expected
     * @return array<string, T|null>
     */
    public function mGetObjects(array $keys, ?string $expected = null): array;
}
