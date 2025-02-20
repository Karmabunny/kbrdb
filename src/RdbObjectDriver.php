<?php

namespace karmabunny\rdb;


interface RdbObjectDriver
{

    /**
     * Set an object.
     *
     * @param string $key
     * @param object $value
     * @param int $ttl milliseconds
     * @return int object size in bytes
     */
    public function setObject(string $key, object $value, $ttl = 0): int;


    /**
     * Get an object from a key.
     *
     * @template T of object
     * @param string $key
     * @param class-string<T>|null $expected
     * @return T|null
     */
    public function getObject(string $key, string $expected = null): ?object;


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
    public function mGetObjects(array $keys, string $expected = null): array;
}
