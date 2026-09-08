<?php
/**
 * @link      https://github.com/Karmabunny
 * @copyright Copyright (c) 2021 Karmabunny
 */

namespace karmabunny\rdb;

/**
 * Static version of Rdb.
 *
 * Extend this and implement the getConfig() method.
 *
 * @method static \Generator<string> prefix(string $prefix, iterable<string> $items)
 * @method static bool match(string $pattern, string $key)
 *
 * @method static void flushAll(bool $async = false)
 * @method static void flushDb(bool $async = false)
 * @method static void flushPrefix(bool $scan = true)
 * @method static bool select(int $database)
 * @method static bool move(string $key, int $database)
 * @method static bool registerSessionHandler(string $prefix = 'session:')
 *
 * @method static int|string|bool|array|null eval(string $script, array $keys = [], array $args = [])
 *
 * @method static int|float|null ttl(string $key, bool $ms = false)
 * @method static bool expire(string $key, int|float $ttl = 0)
 * @method static bool expireAt(string $key, int|float $ttl = 0)
 * @method static bool rename(string $src, string $dst)
 * @method static string|null type(string $key)
 *
 * @method static bool|string|null set(string $key, string $value, int|float $ttl = 0, array $flags = [])
 * @method static int append(string $key, string $value)
 * @method static string|null get(string $key)
 * @method static string|null getRange(string $key, int $from = 0, int $to = -1)
 * @method static (string|null)[] mGet(iterable<string> $keys)
 * @method static bool mSet(string[] $items)
 *
 * @method static int|null sAdd(string $key, ...$values)
 * @method static array|null sMembers(string $key)
 * @method static \Generator<string> sScan(string $key, string $pattern = '*')
 * @method static int|null sRem(string $key, ...$values)
 * @method static int|null sCard(string $key)
 * @method static bool|null sIsMember(string $key, string $value)
 * @method static bool|null sMove(string $src, string $dst, string $value)
 *
 * @method static int|float|null incr(string $key, int|float $amount = 1, string $cast = 'auto')
 * @method static int|null incrBy(string $key, int $amount)
 * @method static float|null incrByFloat(string $key, float $amount)
 * @method static int|float|null decr(string $key, int|float $amount = 1, string $cast = 'auto')
 * @method static int|null decrBy(string $key, int $amount)
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
 * @method static array|null blPop(string[]|string $keys, int|float|null $timeout = null)
 * @method static array|null brPop(string[]|string $keys, int|float|null $timeout = null)
 * @method static string|null brPoplPush(string $src, string $dst, int|float|null $timeout = null)
 *
 * @method static int|null zAdd(string $key, float[] $members)
 * @method static float|null zIncrBy(string $key, float $value, string $member)
 * @method static array|null zRange(string $key, int|string|null $start = null, int|string|null $stop = null, array $flags = [])
 * @method static int|null zRem(string $key, ...$members)
 * @method static int|null zCard(string $key)
 * @method static int|null zCount(string $key, float $min, float $max)
 * @method static float|null zScore(string $key, string $member)
 * @method static int|null zRank(string $key, string $member)
 * @method static int|null zRevRank(string $key, string $member)
 *
 * @method static int|null hDel(string $key, ...$fields)
 * @method static bool|null hExists(string $key, string $field)
 * @method static bool|null hSet(string $key, string $field, mixed $value, bool $replace = true)
 * @method static string|null hGet(string $key, string $field)
 * @method static array|null hGetAll(string $key)
 * @method static int|float|null hIncr(string $key, string $field, int|float $amount = 1, string $cast = 'auto')
 * @method static int|null hIncrBy(string $key, string $field, int $amount)
 * @method static float|null hIncrByFloat(string $key, string $field, float $amount)
 * @method static int|null hStrLen(string $key, string $field)
 * @method static array|null hKeys(string $key)
 * @method static array|null hVals(string $key)
 * @method static int|null hLen(string $key)
 * @method static array|null hmGet(string $key, ...$fields)
 * @method static bool|null hmSet(string $key, array $fields)
 * @method static \Generator<string,string> hScan(string $key, string $pattern = '*')
 *
 * @method static int exists(...$keys)
 * @method static int del(...$keys)
 * @method static string[] keys(string $pattern)
 * @method static \Generator<string> scan(string $pattern)
 * @method static string|null dump(string $key)
 * @method static bool restore(string $key, int $ttl, string $value, array $flags = [])
 * @method static string|null substr(string $key, int $from, int $length = -1)
 * @method static \Generator<string|null> mScan(iterable<string> $keys)
 *
 * @method static string|null inspectObject(string $key)
 * @method static int setObject(string $key, object $value, int|float $ttl = 0)
 * @method static object|null getObject(string $key, string $expected)
 * @method static (object|null)[] mGetObjects(iterable<string> $keys, string $expected, bool $nullish = false)
 * @method static \Generator<object|null> mScanObjects(iterable<string> $keys, string $expected, bool $nullish = false)
 * @method static int[] mSetObjects(object[] $items)
 *
 * @method static int setJson(string $key, mixed $value, int|float $ttl = 0)
 * @method static mixed getJson(string $key, bool $throw = true)
 *
 * @method static int setHash(string $key, array $value)
 * @method static array|null getHash(string $key)
 *
 * @method static int pack(string $key, mixed $value, int|float $ttl = 0)
 * @method static mixed unpack(string $key)
 *
 * @method static RdbLock|null lock(string $key, int|float $wait = 0, int|float $ttl = 60)
 * @method static RdbBucket getBucket(array|string $config)
 *
 * @method static int export(string|resource $file, array|string $config = [])
 * @method static string[] import(string|resource $file, array|string $config = [])
 */
abstract class StaticRdb
{

    /**
     * Create an Rdb configuration.
     *
     * @return RdbConfig
     */
    public static abstract function getConfig(): RdbConfig;


    /**
     * Get a singleton instance of the Rdb helper.
     *
     * @return Rdb
     */
    public static function getInstance(): Rdb
    {
        static $rdb;
        $rdb = $rdb ?? Rdb::create(static::getConfig());
        return $rdb;
    }


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
