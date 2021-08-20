<?php
/**
 * @link      https://github.com/Karmabunny
 * @copyright Copyright (c) 2021 Karmabunny
 */

namespace karmabunny\rdb;

/**
 * Static version of Rdb.
 *
 * Extend this and implement the getInstance() method.
 *
 * @method static bool set(string $key, string $value, int $ttl = 0)
 * @method static string|null get(string $key)
 * @method static int exists(...$keys)
 * @method static int del(...$keys)
 *
 * @method static int sAdd(string $key, ...$values)
 * @method static array sMembers(string $key)
 * @method static int sRem(string $key, ...$values)
 *
 * @method static string[] keys(string $pattern)
 * @method static Generator<string> scan(string $pattern)
 *
 * @method static (string|null)[] mGet(string $key)
 * @method static bool mSet(string[] $items)
 * @method static Generator<string|null> mScan(string[] $keys)
 *
 * @method static int setObject(string $key, object $value)
 * @method static object|null getObject(string $key, string $expected = null)
 * @method static object[] mGetObjects(string[] $keys, string $expected = null)
 * @method static Generator<object> mScanObjects(string[] $keys, string $expected = null)
 * @method static int[] mSetObjects(object[] $items)
 *
 * @method static int setJson(string $key, array|\JsonSerializable $value)
 * @method static array|null getJson(string $key)
 *
 * @method static RdbLock|null lock(string $key, float $wait, float $ttl = 300)
 *
 * @method static RdbBucket getBucket(array|string $config)
 */
abstract class StaticRdb
{

    public static abstract function getInstance(): Rdb;


    /**
     * Yeah look, this is pretty much it. It's all just typing hints.
     *
     * @param mixed $name
     * @param mixed $arguments
     * @return mixed
     */
    public static function __callStatic($name, $arguments)
    {
        $rdb = static::getInstance();
        return $rdb->$name(...$arguments);
    }
}
