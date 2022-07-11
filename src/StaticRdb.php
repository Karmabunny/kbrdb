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
 * @method static Generator<string> prefix(string $prefix, iterable<string> $items)
 *
 * @method static bool set(string $key, string $value, int $ttl = 0)
 * @method static string|null get(string $key)
 * @method static int exists(...$keys)
 * @method static int del(...$keys)
 *
 * @method static int|null ttl(string $key)
 * @method static expire expire(string $key, int $ttl = 0)
 * @method static bool rename(string $src, string $dst)
 * @method static string|null type(string $key)
 *
 * @method static bool sSet(string[] $items)
 * @method static int sAdd(string $key, ...$values)
 * @method static array sMembers(string $key)
 * @method static int sRem(string $key, ...$values)
 * @method static int|null sCard(string $key)
 * @method static bool|null sIsMember(string $key, string $value)
 * @method static bool|null sMove(string $src, string $dst, string $value)
 *
 * @method static int incr(string $key, int $amount = 1)
 * @method static int decr(string $key, int $amount = 1)
 *
 * @method static int|null lPush(string $key, ...$items)
 * @method static int|null rPush(string $key, ...$items)
 * @method static string|null lPop(string $key)
 * @method static string|null rPop(string $key)
 * @method static string|null rPoplPush(string $src, string $dst)
 * @method static array|null lRange(string $key, int $start = 0, int $stop = -1)
 * @method static bool|null lTrim(string $key, int $start = 0, int $stop = -1)
 * @method static int|null lLen(string $key)
 * @method static bool|null lSet(string $key, int $index, string $item)
 * @method static string|null lIndex(string $key, int $index)
 * @method static int|null lRem(string $key, string $item, int $count = 0)
 * @method static array|null blPop($keys, int $timeout = null)
 * @method static array|null brPop($keys, int $timeout = null)
 * @method static string|null brPoplPush(string $src, string $dst, int $timeout = null)
 *
 * @method static string[] keys(string $pattern)
 * @method static Generator<string> scan(string $pattern)
 *
 * @method static (string|null)[] mGet(string $key)
 * @method static bool mSet(string[] $items)
 * @method static Generator<string|null> mScan(iterable<string> $keys)
 *
 * @method static int setObject(string $key, object $value)
 * @method static object|null getObject(string $key, string $expected = null)
 * @method static (object|null)[] mGetObjects(iterable<string> $keys, string $expected = null, bool $nullish = false)
 * @method static Generator<object|null> mScanObjects(iterable<string> $keys, string $expected = null, bool $nullish = false)
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
