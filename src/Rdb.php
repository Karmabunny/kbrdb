<?php
/**
 * @link      https://github.com/Karmabunny
 * @copyright Copyright (c) 2021 Karmabunny
 */

namespace karmabunny\rdb;

use Generator;
use InvalidArgumentException;
use JsonException;

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
    use RdbHelperTrait;


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
     * Does this key match the pattern?
     *
     * @param string $pattern
     * @param string $key
     * @return bool
     */
    public static function match(string $pattern, string $key): bool
    {
        $pattern = str_replace('[^', '[!', $pattern);
        return fnmatch($pattern, $key, FNM_NOESCAPE);
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
     * Change the active database.
     *
     * @param int $database
     * @return bool
     */
    public abstract function select(int $database): bool;


    /**
     * Move a key from the active database to another database.
     *
     * @param string $key
     * @param int $database
     * @return bool
     */
    public abstract function move(string $key, int $database): bool;


    /**
     * Install a session handler.
     *
     * Some adapters may not support session handlers in certain conditions.
     *
     * @param string $prefix
     * @return bool true on a successful install
     */
    public abstract function registerSessionHandler(string $prefix = 'session:'): bool;


    /**
     * Get the TTL for a key.
     *
     * @param string $key
     * @return int|null milliseconds
     */
    public abstract function ttl(string $key): ?int;


    /**
     * Set the expiry/TLL for a key.
     *
     * @param string $key
     * @param int $ttl milliseconds
     * @return bool
     */
    public abstract function expire(string $key, $ttl = 0): bool;


    /**
     * Set the expiry in unix time for a key.
     *
     * @param string $key
     * @param int $ttl milliseconds
     * @return bool
     */
    public abstract function expireAt(string $key, $ttl = 0): bool;


    /**
     * Rename a key.
     *
     * @param string $src
     * @param string $dst
     * @return bool
     */
    public abstract function rename(string $src, string $dst): bool;


    /**
     * Get the type of a key.
     *
     * @param string $key
     * @return string|null one of:
     * - string
     * - list
     * - set
     * - zset
     * - hash
     * - `null` - unknown or missing
     */
    public abstract function type(string $key): ?string;


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
    public abstract function set(string $key, string $value, int $ttl = 0, array $flags = []);


    /**
     * Append a value to a key.
     *
     * @param string $key
     * @param string $value
     * @return int string length after append
     */
    public abstract function append(string $key, string $value): int;


    /**
     * Get a value. Null if missing.
     *
     * @param string $key
     * @return string|null
     */
    public abstract function get(string $key): ?string;


    /**
     * Get a substring of a value. Null if missing.
     *
     * Note, the second parameter is _not_ a 'length' like PHP's `substr`
     * and is an _inclusive_ index.
     *
     * @param string $key
     * @param int $from
     * @param int $to
     * @return string|null
     */
    public abstract function getRange(string $key, int $from = 0, int $to = -1): ?string;


    /**
     * Get many items.
     *
     * The number of returns will always match number of keys.
     *
     * @param iterable<string> $keys
     * @return (string|null)[] Missing keys are null
     */
    public abstract function mGet(iterable $keys): array;


    /**
     * Store many items.
     *
     * This will replace existing items.
     *
     * @param string[] $items [ key => string ]
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
     * @return int number of new items added, `null` if not a set
     */
    public abstract function sAdd(string $key, ...$values): ?int;


    /**
     * Get values of a set.
     *
     * All results will be unique.
     *
     * @param string $key
     * @return string[] set members, null if not a set
     */
    public abstract function sMembers(string $key): ?array;


    /**
     * Scan values of a set.
     *
     * All results will be unique.
     *
     * @param string $key
     * @param string $pattern
     * @return Generator<string> set members
     */
    public abstract function sScan(string $key, string $pattern = '*'): Generator;


    /**
     * Remove an item (or items) from a set.
     *
     * @param string $key
     * @param mixed $values
     * @return int number of items removed, null if not a set
     */
    public abstract function sRem(string $key, ...$values): ?int;


    /**
     * Get the cardinality (size) of a set.
     *
     * @param string $key
     * @return int number of items in the set, null if not a set
     */
    public abstract function sCard(string $key): ?int;


    /**
     * Test if an item is a member of a set.
     *
     * @param string $key
     * @param string $value
     * @return bool true if a member, null if not a set
     */
    public abstract function sIsMember(string $key, string $value): ?bool;


    /**
     * Move an item from one set to another.
     *
     * @param string $src
     * @param string $dst
     * @param string $value
     * @return bool true if moved, or false if not a member, null if not a set
     */
    public abstract function sMove(string $src, string $dst, string $value): ?bool;


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
    public abstract function lPush(string $key, ...$items): ?int;


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
    public abstract function rPush(string $key, ...$items): ?int;


    /**
     * Remove (and return) an item from the start of a list.
     *
     * - aka: LEFT POP
     * - aka: shift()
     *
     * @param string $key
     * @return string|null the item or `null` if empty or not a list
     */
    public abstract function lPop(string $key): ?string;


    /**
     * Remove (and return) an item from the end of a list.
     *
     * - aka: RIGHT POP
     * - aka: pop()
     *
     * @param string $key
     * @return string|null the item or `null` if empty or not a list
     */
    public abstract function rPop(string $key): ?string;


    /**
     * Remove (and return) an item from the end of a list.
     *
     * aka: BLOCKING RIGHT POP -> LEFT PUSH
     *
     * Note, although this is deprecated in redis 6.2 it will indefinitely be
     * supported here, if removed, with a polyfill via `LMOVE`.
     *
     * Also note, if either key isn't a list it will also return `null`.
     *
     * @param string $src
     * @param string $dst
     * @return string|null item being moved or `null` if src list is empty
     */
    public abstract function rPoplPush(string $src, string $dst): ?string;


    /**
     * Retrieve items from a list.
     *
     * aka: LIST RANGE
     *
     * Negative indexes are circular and wrap around to the end of the list.
     * E.g. `[1,2,3] => -1 is 3`
     *
     * Out of range indexes are _not_ errors. A too-large index is treated as
     * and 'end of list' index, aka `-1`.
     *
     * @param string $key
     * @param int $start
     * @param int $stop
     * @return string[]|null range items or `null` if not a list.
     */
    public abstract function lRange(string $key, int $start = 0, int $stop = -1): ?array;


    /**
     * Remove items from a list.
     *
     * aka: LIST TRIM
     *
     * Negative indexes are circular and wrap around to the end of the list.
     * E.g. `[1,2,3] => -1 is 3`
     *
     * Out of range indexes are _not_ errors. A too-large index is treated as
     * and 'end of list' index, aka `-1`.
     *
     * @param string $key
     * @param int $start
     * @param int $stop
     * @return bool|null
     */
    public abstract function lTrim(string $key, int $start = 0, int $stop = -1): ?bool;


    /**
     * Get the length of a list.
     *
     * aka: LIST LENGTH
     *
     * @param string $key
     * @return int|null list length or `null` if not a list.
     */
    public abstract function lLen(string $key): ?int;


    /**
     * Store a item at this index of a list.
     *
     * aka: LIST SET
     *
     * @param string $key
     * @param int $index
     * @param string $item
     * @return bool|null true if set, false if out of range, null if not a list
     */
    public abstract function lSet(string $key, int $index, string $item): ?bool;


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
    public abstract function lIndex(string $key, int $index): ?string;


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
     * @return int|null number of removed items, or `null` if not a list
     */
    public abstract function lRem(string $key, string $item, int $count = 0): ?int;


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
    public abstract function blPop($keys, int $timeout = null): ?array;


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
    public abstract function brPop($keys, int $timeout = null): ?array;


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
    public abstract function brPoplPush(string $src, string $dst, int $timeout = null): ?string;


    /**
     * Add items to a sorted set.
     *
     * One must indicate a score for each member, the format looks like:
     *
     * ```
     * [
     *   'a' => 5,
     *   'b' => 1,
     *   'c' => 10,
     * ]
     * ```
     *
     * If the set exists, the member values are updated - these updated members
     * are excluded from the return count. Otherwise this will create a new
     * sorted set.
     *
     * @param string $key
     * @param float[] $members [ member => score ]
     * @return int number of elements added
     */
    public abstract function zAdd(string $key, array $members): int;


    /**
     * Increment a member's score in a sorted set.
     *
     * This will create a new sorted set if it doesn't exist.
     *
     * @param string $key
     * @param float $value an amount to update by, can be negative
     * @param string $member
     * @return float the value after updated
     */
    public abstract function zIncrBy(string $key, float $value, string $member): float;


    /**
     * Get members from a sorted set.
     *
     * This supports all alternate flags:
     *
     * | Mode    | Flags                  |
     * |---------|------------------------|
     * | --      | rev, withscores        |
     * | byscore | rev, limit, withscores |
     * | bylex   | rev, limit             |
     *
     * The limit option can be specified as either:
     * - numeric: `[0, 10]`
     * - keyed: `['offset' => 0, 'count' => 10]`
     *
     * The default the start/stop range will return all members.
     *
     * For `bylex` the start/stop is inclusive by default unless overridden
     * with the `[ or (` syntax.
     *
     * Note, if the set does not exist it will return an empty array. This
     * returns null only if the key is not a sorted set.
     *
     * @param string $key
     * @param int|string|null $start defaults:
     *   - rank: `0`
     *   - byscore: `-inf`
     *   - bylex: `-` (inf)
     * @param int|string|null $stop defaults:
     *   - rank: `-1` (circular, meaning end-of-set)
     *   - byscore: `+inf`
     *   - bylex: `+` (inf)
     * @param array $flags
     *  - withscores: include the score with each member (not available for bylex)
     *  - rev: reverse the order
     *  - limit: limit the results (only for byscore + bylex)
     *  - byscore: filter by score
     *  - bylex: filter by lexicographical order
     *
     * @return null|array members are either:
     *  - a numeric list, ordered by their score
     *  - a keyed array like `[ member => score ]` (withscores)
     *  - `null` if the key is not a sorted set
     */
    public abstract function zRange(string $key, $start = null, $stop = null, array $flags = []): ?array;


    /**
     * Remove members from a sorted set.
     *
     * @param string $key
     * @param string[]|string $members
     * @return int number of removed items
     */
    public abstract function zRem(string $key, ...$members): int;


    /**
     * Get the number of members in a sorted set.
     *
     * @param string $key
     * @return int|null number of items
     */
    public abstract function zCard(string $key): ?int;


    /**
     * Get the number of members within a range of scores within a sorted set.
     *
     * This has the same semantics as zRange 'by score'.
     *
     * @param string $key
     * @param float $min
     * @param float $max
     * @return int|null number of items
     */
    public abstract function zCount(string $key, float $min, float $max): ?int;


    /**
     * Get the score of a member in a sorted set.
     *
     * @param string $key
     * @param string $member
     * @return float|null score
     */
    public abstract function zScore(string $key, string $member): ?float;


    /**
     * Get the rank of a member in a sorted set.
     *
     * @param string $key
     * @param string $member
     * @return int|null rank, 0-indexed
     */
    public abstract function zRank(string $key, string $member): ?int;


    /**
     * Get the reversed rank of a member in a sorted set.
     *
     * @param string $key
     * @param string $member
     * @return int|null rank, 0-indexed
     */
    public abstract function zRevRank(string $key, string $member): ?int;


    /**
     * Do these keys exist?
     *
     * @param string|iterable<string> $keys
     * @return int number of matches
     */
    public abstract function exists(...$keys): int;


    /**
     * Delete a key, or a list of keys.
     *
     * @param string|iterable<string> $keys
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
     *
     * @param string $key
     * @return string|null
     */
    public abstract function dump(string $key): ?string;


    /**
     *
     * @param string $key
     * @param int $ttl
     * @param string $value
     * @param array $flags
     * @return bool
     */
    public abstract function restore(string $key, int $ttl, string $value, array $flags = []): bool;


    /**
     * A wrapper around getRange because it's spooky.
     *
     * This behaves the same as PHP's `substr()`.
     *
     * @param string $key
     * @param int $from
     * @param int $length
     * @return null|string
     */
    public function substr(string $key, int $from, int $length = -1): ?string
    {
        if ($length == 0) {
            return $this->exists($key) ? '' : null;
        }

        $to = $length;

        if ($length !== -1) {
            $to -= 1;
        }

        if ($length > 0) {
            $to += $from;
        }

        return $this->getRange($key, $from, $to);
    }


    /**
     * Bulk fetch via a list of keys.
     *
     * @param iterable<string> $keys
     * @return Generator<string|null> [ key => item ]
     */
    public function mScan(iterable $keys): Generator
    {
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
     * @param iterable<string> $keys Non-prefixed keys
     * @param string|null $expected Ensure all results inherits/is of this type
     * @param bool $nullish (false) return empty values
     * @return (object|null)[] [ key => item ]
     * @throws InvalidArgumentException
     */
    public function mGetObjects($keys, string $expected = null, bool $nullish = false): array
    {
        if (
            $expected
            and !class_exists($expected)
            and !interface_exists($expected)
        ) {
            throw new InvalidArgumentException('Not a class or interface: ' . $expected);
        }

        $keys = self::normalizeIterable($keys, false);

        if (empty($keys)) {
            return [];
        }

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
    public function mScanObjects(iterable $keys, string $expected = null, bool $nullish = false): Generator
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
     * @param object[] $items [ key => string ]
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
        unset($item);

        $ok = $this->mSet($items);

        if (!$ok) {
            return [];
        }

        return $sizes;
    }


    /**
     * Store a JSON document in a key.
     *
     * @param string $key
     * @param mixed $value
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
     * @return mixed|null
     */
    public function getJson(string $key)
    {
        $value = $this->get($key);
        if ($value === null) {
            return null;
        }

        $value = json_decode($value, true);

        $error = json_last_error();
        if ($error !== JSON_ERROR_NONE) {
            throw new JsonException(json_last_error_msg(), $error);
        }

        return $value;
    }


    /**
     * Create a lock.
     *
     * This will block for `$wait` milliseconds until the lock is released.
     * It will then return a _new_ lock if available. If the resource is still
     * locked it returns null.
     *
     * Given a `$wait` of zero (default), this will return immediately if
     * the resource is not available.
     *
     * The `$ttl` will auto-expire a lock should it somehow not self-destruct.
     * If your code runs longer than 1 minute, it's recommended to bump this up.
     *
     * @param string $key
     * @param int $wait milliseconds
     * @param int $ttl milliseconds (default 1 minute)
     * @return RdbLock|null
     */
    public function lock(string $key, int $wait = 0, int $ttl = 60000): ?RdbLock
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


    public function export($file, string $pattern = '*')
    {
        $export = new RdbExport($this, $pattern);
        $export->export($file);
    }


    public function import($file, string $pattern = '*'): array
    {
        $import = new RdbImport($this, $pattern);
        $import->import($file);
        return $import->errors;
    }
}
