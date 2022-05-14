<?php
/**
 * @link      https://github.com/Karmabunny
 * @copyright Copyright (c) 2021 Karmabunny
 */

namespace karmabunny\rdb;

use Generator;
use InvalidArgumentException;
use JsonException;
use JsonSerializable;


/**
 * Rdb is a wrapper around other popular redis libraries.
 *
 * It tries to create a unified interface for different adapters
 * (php-redis, predis, credis).
 *
 * It also contains some useful helpers:
 * - prefixing
 * - object serialisation
 * - scanning generators
 * - locking
 * - rate limiting
 *
 * Feel free to add to it.
 *
 * @package karmabunny\rdb
 */
abstract class Rdb
{
    const ADAPTERS = [
        RdbConfig::TYPE_PHP_REDIS => PhpRedisAdapter::class,
        RdbConfig::TYPE_PREDIS => PredisAdapter::class,
        RdbConfig::TYPE_CREDIS => CredisAdapter::class,
    ];


    /** @var RdbConfig */
    public $config;


    /**
     * Build the config.
     *
     * This _must_ be called first in any child constructors.
     *
     * @param RdbConfig|array $config
     * @throws InvalidArgumentException
     */
    protected function __construct($config)
    {
        if ($config instanceof RdbConfig) {
            $this->config = clone $config;
        }
        else {
            $this->config = new RdbConfig($config);
        }
    }


    /**
     * Create an Rdb client for the given config.
     *
     * This will build a client with the appropriate adapter - as defined
     * by the config. If not specified, the default adapter is TYPE_PREDIS.
     *
     * @param RdbConfig|array $config
     * @return Rdb
     * @throws InvalidArgumentException
     */
    public static function create($config): Rdb
    {
        if (is_array($config)) {
            $config = new RdbConfig($config);
        }

        $adapter = self::ADAPTERS[$config->adapter] ?? null;
        if (!$adapter) {
            throw new InvalidArgumentException('Invalid rdb adapter: ' . $config->adapter);
        }

        return new $adapter($config);
    }


    /**
     * Apply a prefix to all items.
     *
     * @param string $prefix
     * @param iterable<string> $items
     * @return Generator<string>
     */
    public static function prefix(string $prefix, $items): Generator
    {
        foreach ($items as $item) {
            yield $prefix . $item;
        }
    }


    /**
     * Strip the prefix from list of keys.
     *
     * @param string $key
     * @return string
     */
    protected function stripPrefix(string $key): string
    {
        if ($this->config->prefix) {
            return preg_replace("/^{$this->config->prefix}/", '', $key);
        }
        else {
            return $key;
        }
    }


    /**
     * Flatten an array input.
     *
     * @param array $items
     * @return array
     */
    protected static function flattenArrays(array $items): array
    {
        $output = [];
        array_walk_recursive($items, function($item) use (&$output) {
            $output[] = $item;
        });
        return $output;
    }


    /**
     * Parse + normalise set() flags.
     *
     * @param array $flags
     * @return array
     */
    protected static function parseFlags(array $flags): array
    {
        // Defaults.
        $output = [
            'keep_ttl' => null,
            'time_at' => null,
            'get_set' => null,
            'replace' => null,
        ];

        // Normalise.
        foreach ($flags as $key => $value) {
            if (is_numeric($key)) {
                $output[strtolower($value)] = true;
            }
            else {
                $output[strtolower($key)] = $value;
            }
        }

        // For those smarty pants that write 'replace => NX/XX'.
        if ($output['replace'] === 'NX') {
            $output['replace'] = false;
        }
        else if ($output['replace'] === 'XX') {
            $output['replace'] = true;
        }
        else if (!is_bool($output['replace'])) {
            $output['replace'] = null;
        }

        return $output;
    }


    /**
     * Store a value at a key.
     *
     * Optionally specify a TTL that will cause the key to automatically
     * delete after a period of milliseconds.
     *
     * Flags are an array for modifying behaviour, for example:
     *
     * ```
     * $rdb->set('key', 'value', 1000, [
     *    'time_at',           // PXAT
     *    'get_set',           // GET
     *    'replace' => false,  // NX
     * ]);
     * ```
     *
     * @param string $key
     * @param string $value
     * @param int $ttl milliseconds
     * @param array $flags
     *  - keep_ttl: retain the TTL when replacing a key
     *  - time_at: the TTL is an expiry unix time in milliseconds
     *  - get_set: get the old value before setting the new one
     *  - replace: `bool` - only set if it (not) exists (true: XX, false: NX)
     * @return bool|string|null
     *    - string|null if the GET flag is set
     *    - bool for everything else
     */
    public abstract function set(string $key, string $value, $ttl = 0, $flags = []);


    /**
     * Get a value. Null if missing.
     *
     * @param string $key
     * @return string|null
     */
    public abstract function get(string $key);


    /**
     * Get many items.
     *
     * The number of returns will always match number of keys.
     *
     * @param string[] $keys
     * @return (string|null)[] Missing keys are null
     */
    public abstract function mGet(array $keys): array;


    /**
     * Store many items.
     *
     * This will replace existing items.
     *
     * @param string[] $items key => string
     * @return bool always true, unless items is empty.
     */
    public abstract function mSet(array $items): bool;


    /**
     * Add a value (or many) to a set.
     *
     * Sets by nature are unique. Adding a new item will not create a
     * duplicate entry.
     *
     * @param string $key
     * @param mixed $values
     * @return int number of new items added
     */
    public abstract function sAdd(string $key, ...$values): int;


    /**
     * Get values of a set.
     *
     * All results will be unique.
     *
     * @param string $key
     * @return array
     */
    public abstract function sMembers(string $key): array;


    /**
     * Remove an item (or items) from a set.
     *
     * @param string $key
     * @param mixed $values
     * @return int number of items removed
     */
    public abstract function sRem(string $key, ...$values): int;


    /**
     * Get the cardinality (size) of a set.
     *
     * @param string $key
     * @return int number of items in the set
     */
    public abstract function sCard(string $key): int;


    /**
     * Test if an item is a member of a set.
     *
     * @param string $key
     * @param string $value
     * @return bool
     */
    public abstract function sIsMember(string $key, string $value): bool;


    /**
     * Move an item from one set to another.
     *
     * @param string $src
     * @param string $dst
     * @param string $value
     * @return bool true if moved, or false if not a member
     */
    public abstract function sMove(string $src, string $dst, string $value): bool;


    /**
     * Increment a value by X.
     *
     * @param string $key
     * @param int $amount
     * @return int the value after incrementing
     */
    public abstract function incr(string $key, int $amount = 1): int;


    /**
     * Decrement a value by X.
     *
     * @param string $key
     * @param int $amount
     * @return int the value after decrementing
     */
    public abstract function decr(string $key, int $amount = 1): int;


    /**
     * Add items to the start of a list.
     *
     * - aka: LEFT PUSH
     * - aka: unshift()
     *
     * @param string $key
     * @param string $items
     * @return int|null the list length after after pushing
     */
    public abstract function lPush(string $key, ...$items);


    /**
     * Add items to the end of a list.
     *
     * - aka: RIGHT PUSH
     * - aka: append()
     *
     * @param string $key
     * @param string $items
     * @return int|null the list length after after pushing
     */
    public abstract function rPush(string $key, ...$items);


    /**
     * Remove (and return) an item from the start of a list.
     *
     * - aka: LEFT POP
     * - aka: shift()
     *
     * @param string $key
     * @return string|null the item or `null` if empty
     */
    public abstract function lPop(string $key);


    /**
     * Remove (and return) an item from the end of a list.
     *
     * - aka: RIGHT POP
     * - aka: pop()
     *
     * @param string $key
     * @return string|null the item or `null` if empty
     */
    public abstract function rPop(string $key);


    /**
     * Remove (and return) an item from the end of a list.
     *
     * aka: BLOCKING RIGHT POP -> LEFT PUSH
     *
     * Note, although this is deprecated in redis 6.2 it will indefinitely be
     * supported here, if removed, with a polyfill via `LMOVE`.
     *
     * @param string $src
     * @param string $dst
     * @return string|null item being moved or `null` if src list is empty
     */
    public abstract function rPoplPush(string $src, string $dst);


    /**
     * Retrieve items from a list.
     *
     * aka: LIST RANGE
     *
     * Note, start/stop can be negative - they behave circularly.
     *
     * @param string $key
     * @param int $start
     * @param int $stop
     * @return string[]
     */
    public abstract function lRange(string $key, int $start = 0, int $stop = -1): array;


    /**
     * Remove items from a list.
     *
     * aka: LIST TRIM
     *
     * Note, start/stop can be negative - they behave circularly.
     *
     * @param string $key
     * @param int $start
     * @param int $stop
     * @return bool
     */
    public abstract function lTrim(string $key, int $start = 0, int $stop = -1): bool;


    /**
     * Get the length of a list.
     *
     * aka: LIST LENGTH
     *
     * @param string $key
     * @return int|null list length or `null` if not a list.
     */
    public abstract function lLen(string $key);


    /**
     * Store a item at this index of a list.
     *
     * aka: LIST SET
     *
     * @param string $key
     * @param int $index
     * @param string $item
     * @return bool
     */
    public abstract function lSet(string $key, int $index, string $item): bool;


    /**
     * Get an item from a list at this index.
     *
     * aka: LIST INDEX
     *
     * Note, `index` can be negative - it behaves circularly.
     *
     * @param string $key
     * @param int $index
     * @return string|null the item or `null` if out-of-range
     */
    public abstract function lIndex(string $key, int $index);


    /**
     * Remove an item from a list.
     *
     * aka: LIST REMOVE
     *
     * - count > 0: Remove elements equal to element moving from head to tail.
     * - count < 0: Remove elements equal to element moving from tail to head.
     * - count = 0: Remove all elements equal to element.
     *
     * @param string $key
     * @param string $item
     * @param int $count
     * @return int number of removed items
     */
    public abstract function lRem(string $key, string $item, int $count = 0): int;


    /**
     * Remove (and return) an item from the start of a list. If there are no
     * items then this method will block until an item is added.
     *
     * aka: BLOCKING LET POP
     *
     * @param string[]|string $keys
     * @param int $timeout in seconds - if unset, the config/connection timeout
     * @return string[]|null [key, item] or `null` if the timeout occurs
     */
    public abstract function blPop($keys, int $timeout = null);


    /**
     * Remove (and return) an item from the end of a list. If there are no
     * items then this method will block until an item is added.
     *
     * aka: BLOCKING RIGHT POP
     *
     * @param string[]|string $keys
     * @param int $timeout in seconds - if unset, the config/connection timeout
     * @return string[]|null [key, item] or `null` if the timeout occurs
     */
    public abstract function brPop($keys, int $timeout = null);


    /**
     * Remove (and return) an item from the end of a list. If there are no
     * items then this method will block until an item is added.
     *
     * aka: BLOCKING RIGHT POP -> LEFT PUSH
     *
     * Note, although this is deprecated in redis 6.2 it will indefinitely be
     * supported here, if removed, with a polyfill via `BLMOVE`.
     *
     * @param string $src
     * @param string $dst
     * @param int $timeout in seconds - if unset, the config/connection timeout
     * @return string|null item being moved or `null` if the timeout occurs
     */
    public abstract function brPoplPush(string $src, string $dst, int $timeout = null);


    /**
     * Do these keys exist?
     *
     * @param string|string[] $keys
     * @return int number of matches
     */
    public abstract function exists(...$keys): int;


    /**
     * Delete a key, or a list of keys.
     *
     * @param string|string[] $keys
     * @return int number of keys deleted
     */
    public abstract function del(...$keys): int;


    /**
     * Get a list of keys that match a pattern.
     *
     * You _should_ be using the `scan()` method.
     *
     * @param string $pattern
     * @return string[]
     */
    public abstract function keys(string $pattern): array;


    /**
     * Iterate over a set of pattern to find a list of keys.
     *
     * Use this over the `keys()` method.
     *
     * @param string $pattern
     * @return Generator<string>
     */
    public abstract function scan(string $pattern): Generator;



    /**
     * Bulk fetch via a list of keys.
     *
     * @param string[] $keys
     * @return Generator<string|null> [ key => item ]
     */
    public function mScan(array $keys): Generator
    {
        if (empty($keys)) return [];

        $chunk = [];

        foreach ($keys as $key) {
            $chunk[] = $key;
            if (count($chunk) !== $this->config->chunk_size) continue;

            $items = $this->mGet($chunk);
            yield from $items;
            $chunk = [];
        }

        if (!empty($chunk)) {
            $items = $this->mGet($chunk);
            yield from $items;
        }
    }



    /**
     * Store an object at this key.
     *
     * @param string $key
     * @param object $value
     * @param int $ttl milliseconds
     * @return int object size in bytes
     */
    public function setObject(string $key, $value, $ttl = 0): int
    {
        $value = serialize($value);
        if (!$this->set($key, $value, $ttl)) return 0;
        return strlen($value);
    }


    /**
     * Get an object at this key.
     *
     * This returns null if the key is empty or the object doesn't match the
     * 'expected' type.
     *
     * @param string $key
     * @param string|null $expected Ensure the result inherits/is this type
     * @return object|null
     * @throws InvalidArgumentException
     */
    public function getObject(string $key, string $expected = null)
    {
        if (
            $expected
            and !class_exists($expected)
            and !interface_exists($expected)
        ) {
            throw new InvalidArgumentException('Not a class or interface: ' . $expected);
        }

        $value = @unserialize($this->get($key));
        if ($value === false) return null;
        if (!is_object($value)) return null;

        if (
            // @phpstan-ignore-next-line : doesn't like string classes.
            $expected and
            get_class($value) !== $expected and
            !is_subclass_of($value, $expected, false)
        ) return null;

        return $value;
    }


    /**
     * Bulk fetch objects via a list of keys.
     *
     * Empty keys are filtered out - if `nullish` is false (default).
     *
     * @param string[] $keys Non-prefixed keys
     * @param string|null $expected Ensure all results inherits/is of this type
     * @param bool $nullish (false) return empty values
     * @return (object|null)[] [ key => item ]
     * @throws InvalidArgumentException
     */
    public function mGetObjects(array $keys, string $expected = null, bool $nullish = false): array
    {
        if (
            $expected
            and !class_exists($expected)
            and !interface_exists($expected)
        ) {
            throw new InvalidArgumentException('Not a class or interface: ' . $expected);
        }

        // Fix sequential indexes.
        $keys = array_values($keys);

        if (empty($keys)) return [];

        $items = $this->mGet($keys);
        $output = [];

        if ($nullish) {
            $output = array_fill_keys($keys, null);
        }

        foreach ($items as $key => $item) {
            $item = @unserialize($item) ?: null;

            if (!$key or !$item) continue;

            if (!is_object($item)) continue;

            if (
                // @phpstan-ignore-next-line : doesn't like string classes.
                $expected and
                get_class($item) !== $expected and
                !is_subclass_of($item, $expected, false)
            ) continue;

            $output[$key] = $item;
        }

        return $output;
    }


    /**
     * Bulk fetch objects via a list of keys.
     *
     * Empty keys are filtered out - if `nullish` is false (default).
     *
     * @param iterable $keys Non-prefixed keys
     * @param string|null $expected Ensure all results is of this type
     * @param bool $nullish (false) return empty values
     * @return Generator<object|null> [ key => item ]
     */
    public function mScanObjects($keys, string $expected = null, bool $nullish = false): Generator
    {
        if (
            $expected
            and !class_exists($expected)
            and !interface_exists($expected)
        ) {
            throw new InvalidArgumentException('Not a class or interface: ' . $expected);
        }

        $chunk = [];

        foreach ($keys as $key) {
            // Build a chunk of keys.
            $chunk[] = $key;
            if (count($chunk) !== $this->config->chunk_size) continue;

            // Fetch and yield them.
            $items = $this->mGetObjects($chunk, $expected, $nullish);
            yield from $items;
            $chunk = [];
        }

        // Also emit those leftovers.
        if (!empty($chunk)) {
            $items = $this->mGetObjects($chunk, $expected, $nullish);
            yield from $items;
        }
    }


    /**
     * Bulk set an array of object.
     *
     * @param object[] $items
     * @return int[] [ key => int ] object sizes in bytes
     */
    public function mSetObjects(array $items): array
    {
        if (empty($items)) return [];

        $sizes = [];

        /** @var string[] $items */
        foreach ($items as $key => &$item) {
            $item = serialize($item);
            $sizes[$key] = strlen($item);
        }

        if (!$this->mSet($items)) {
            return [];
        }

        return $sizes;
    }


    /**
     * Store a JSON document in a key.
     *
     * @param string $key
     * @param array|JsonSerializable $value
     * @param int $ttl milliseconds
     * @return int
     */
    public function setJson(string $key, $value, $ttl = 0): int
    {
        $value = json_encode($value);
        if (!$this->set($key, $value, $ttl)) return 0;
        return strlen($value);
    }


    /**
     * Get a JSON document from this key.
     *
     * @param string $key
     * @return array|null
     */
    public function getJson(string $key)
    {
        $out = json_decode($this->get($key) ?? 'null', true);

        $error = json_last_error();
        if ($error !== JSON_ERROR_NONE) {
            throw new JsonException(json_last_error_msg(), $error);
        }

        return $out;
    }


    /**
     * Create a lock.
     *
     * This will block for `$wait` seconds until the lock is released.
     * It will then return a _new_ lock if available. If the resource is still
     * locked it returns null.
     *
     * The `$ttl` will auto-expire a lock should it somehow not self-destruct.
     * If your code runs longer than 1 minute, it's recommended to bump this up.
     *
     * @param string $key
     * @param int $wait milliseconds
     * @param int $ttl milliseconds (default 1 minute)
     * @return RdbLock|null
     */
    public function lock(string $key, int $wait = 0, int $ttl = 60000)
    {
        return RdbLock::acquire($this, $key, (int) $wait, (int) $ttl);
    }


    /**
     * Create or fetch a leaky bucket for rate limiting.
     *
     * @param array|string $config
     * @return RdbBucket
     */
    public function getBucket($config): RdbBucket
    {
        return new RdbBucket($this, $config);
    }
}
