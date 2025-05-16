<?php
/**
 * @link      https://github.com/Karmabunny
 * @copyright Copyright (c) 2021 Karmabunny
 */

namespace karmabunny\rdb;

use Exception;
use Generator;
use InvalidArgumentException;
use JsonException;
use MessagePack\MessagePack;

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


    const CAST_AUTO = 'auto';
    const CAST_FLOAT = 'float';
    const CAST_INTEGER = 'integer';


    /** @var RdbConfig */
    public $config;


    /** @var RdbObjectDriver */
    public $driver;


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

        $class = $this->config->object_driver;
        $this->driver = new $class($this);
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
     * Convert string/int/float to int/float with a preference mode thing.
     *
     * @param mixed $amount
     * @param string $mode
     * @return int|float
     */
    protected function cast($amount, string $mode)
    {
        if ($mode === self::CAST_FLOAT) {
            return (float) $amount;
        }

        if ($mode === self::CAST_INTEGER) {
            return (int) $amount;
        }

        // Happily convert objects with a __toString method.
        if (is_object($amount)) {
            $amount = (string) $amount;
        }

        if (!is_numeric($amount)) {
            return 0;
        }

        // Auto cast for strings by detecting a decimal point.
        if (is_string($amount)) {
            return strpos($amount, '.') !== false
                ? (float) $amount
                : (int) $amount;
        }

        return $amount;
    }


    /**
     *
     * @param bool $async
     * @return void
     */
    public abstract function flushAll(bool $async = false);


    /**
     *
     * @param bool $async
     * @return void
     */
    public abstract function flushDb(bool $async = false);


    /**
     *
     * @param bool $scan
     * @return void
     */
    public function flushPrefix(bool $scan = true)
    {
        $keys = $scan ? $this->scan('*') : $this->keys('*');
        $this->del($keys);
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
     * @param string $prefix
     * @return bool true on a successful install
     */
    public function registerSessionHandler(string $prefix = 'session:'): bool
    {
        $session = new RdbSessionHandler($this, [
            'prefix' => $prefix,
        ]);

        return session_set_save_handler($session, true);
    }


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
     * This is a wrapper around `incrBy` and `incrByFloat` and will use either
     * depending on the given type. This behaviour can be force either way with
     * the 'cast' param.
     *
     * @param string $key
     * @param int|float|string $amount
     * @param string $cast one of: 'auto', 'float', 'integer'
     * @return int|float the value after incrementing
     */
    public function incr(string $key, $amount = 1, $cast = self::CAST_AUTO)
    {
        $amount = self::cast($amount, $cast);

        if (is_float($amount)) {
            return $this->incrByFloat($key, $amount);
        }
        else {
            return $this->incrBy($key, $amount);
        }
    }


    /**
     * Increment a value by X (integer).
     *
     * @param string $key
     * @param int $amount
     * @return int the value after incrementing
     */
    public abstract function incrBy(string $key, int $amount): int;


    /**
     * Increment a value by X (float).
     *
     * @param string $key
     * @param float $amount
     * @return float the value after incrementing
     */
    public abstract function incrByFloat(string $key, float $amount): float;


    /**
     * Decrement a value by X.
     *
     * This will use `decrBy` for integers and `incrByFloat` (negative value)
     * for floats.
     *
     * @param string $key
     * @param int|float|string $amount
     * @param string $cast one of: 'auto', 'float', 'integer'
     * @return int|float the value after decrementing
     */
    public function decr(string $key, $amount = 1, $cast = self::CAST_AUTO)
    {
        $amount = self::cast($amount, $cast);

        if (is_float($amount)) {
            return $this->incrByFloat($key, -1 * $amount);
        }
        else {
            return $this->decrBy($key, $amount);
        }
    }


    /**
     * Decrement a value by X (int).
     *
     * @param string $key
     * @param int $amount
     * @return int the value after decrementing
     */
    public abstract function decrBy(string $key, int $amount): int;


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
    public abstract function blPop($keys, ?int $timeout = null): ?array;


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
    public abstract function brPop($keys, ?int $timeout = null): ?array;


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
    public abstract function brPoplPush(string $src, string $dst, ?int $timeout = null): ?string;


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
     * Delete one or more hash fields.
     *
     * @param string $key
     * @param string ...$fields
     * @return int Number of fields removed
     */
    public abstract function hDel(string $key, ...$fields): int;


    /**
     * Check if a hash field exists.
     *
     * @param string $key
     * @param string $field
     * @return bool True if field exists
     */
    public abstract function hExists(string $key, string $field): bool;


    /**
     * Set the value of a hash field.
     *
     * @param string $key
     * @param string $field
     * @param mixed $value
     * @param bool $replace use `hSetNx` if false
     * @return bool
     */
    public abstract function hSet(string $key, string $field, $value, bool $replace = true): bool;


    /**
     * Get the value of a hash field.
     *
     * @param string $key
     * @param string $field
     * @return string|null value or null
     */
    public abstract function hGet(string $key, string $field): ?string;


    /**
     * Get all fields and values in a hash.
     *
     * @param string $key
     * @return array|null [ field => value ]
     */
    public abstract function hGetAll(string $key): ?array;


    /**
     * Increment a hash field value by X.
     *
     * This is a wrapper around `hIncrBy` and `hIncrByFloat` and will use either
     * depending on the given type. This behaviour can be forced either way with
     * the 'cast' param.
     *
     * @param string $key
     * @param string $field
     * @param int|float|string $amount
     * @param string $cast one of: 'auto', 'float', 'integer'
     * @return int|float the value after incrementing
     */
    public function hIncr(string $key, string $field, $amount = 1, $cast = self::CAST_AUTO)
    {
        $amount = self::cast($amount, $cast);

        if (is_float($amount)) {
            return $this->hIncrByFloat($key, $field, $amount);
        }
        else {
            return $this->hIncrBy($key, $field, $amount);
        }
    }


    /**
     * Increment a hash field by a number (integer).
     *
     * @param string $key
     * @param string $field
     * @param int $amount Amount to increment by
     * @return int New value after increment
     */
    public abstract function hIncrBy(string $key, string $field, int $amount): int;


    /**
     * Increment a hash field by a number (integer).
     *
     * @param string $key
     * @param string $field
     * @param float $amount Amount to increment by
     * @return float New value after increment
     */
    public abstract function hIncrByFloat(string $key, string $field, float $amount): float;


    /**
     * Get the string length of a hash field's value.
     *
     * @param string $key
     * @param string $field
     * @return int|null Length of the value string, or null if field does not exist
     */
    public abstract function hStrLen(string $key, string $field): ?int;


    /**
     * Get all field names in a hash.
     *
     * @param string $key
     * @return array|null Array of field names, or null if the key is not a hash
     */
    public abstract function hKeys(string $key): ?array;


    /**
     * Get all values in a hash.
     *
     * @param string $key
     * @return array|null values of the hash (not keyed), or null if the key is not a hash
     */
    public abstract function hVals(string $key): ?array;


    /**
     * Get the number of fields in a hash.
     *
     * @param string $key
     * @return int Number of fields
     */
    public abstract function hLen(string $key): int;


    /**
     * Get the values of multiple hash fields.
     *
     * @param string $key
     * @param string ...$fields
     * @return array|null Array of values, or null if the key is not a hash
     */
    public abstract function hmGet(string $key, ...$fields): ?array;


    /**
     * Set multiple hash fields to multiple values.
     *
     * @param string $key
     * @param array $fields Array of field-value pairs
     * @return bool True if successful
     */
    public abstract function hmSet(string $key, array $fields): bool;


    /**
     * Incrementally iterate over hash fields and values.
     *
     * @param string $key
     * @param string $pattern Pattern to match field names
     * @return Generator<string,string> Generator yielding field-value pairs
     */
    public abstract function hScan(string $key, string $pattern = '*'): Generator;


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
        return $this->driver->setObject($key, $value, $ttl);
    }


    /**
     * Get an object at this key.
     *
     * This returns null if the key is empty or the object doesn't match the
     * 'expected' type.
     *
     * IMPORTANT: the `$expected` parameter will become mandatory in v2.
     *
     * @param string $key
     * @param string|null $expected Ensure the result inherits/is this type
     * @return object|null
     * @throws InvalidArgumentException
     */
    public function getObject(string $key, ?string $expected = null)
    {
        if (
            $expected
            and !class_exists($expected)
            and !interface_exists($expected)
        ) {
            throw new InvalidArgumentException('Not a class or interface: ' . $expected);
        }

        return $this->driver->getObject($key, $expected);
    }


    /**
     * Bulk fetch objects via a list of keys.
     *
     * Empty keys are filtered out - if `nullish` is false (default).
     *
     * IMPORTANT: the `$expected` parameter will become mandatory in v2.
     *
     * @param iterable<string> $keys Non-prefixed keys
     * @param string|null $expected Ensure all results inherits/is of this type
     * @param bool $nullish (false) return empty values
     * @return (object|null)[] [ key => item ]
     * @throws InvalidArgumentException
     */
    public function mGetObjects($keys, ?string $expected = null, bool $nullish = false): array
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

        $items = $this->driver->mGetObjects($keys, $expected);

        if ($nullish) {
            $items = $items + array_fill_keys($keys, null);
        }

        return $items;
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
     * @throws InvalidArgumentException
     */
    public function mScanObjects(iterable $keys, ?string $expected = null, bool $nullish = false): Generator
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
        if (empty($items)) {
            return [];
        }

        return $this->driver->mSetObjects($items);
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

        $error = json_last_error();
        if ($error !== JSON_ERROR_NONE) {
            throw new JsonException(json_last_error_msg(), $error);
        }

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
     * Store an array in a hash.
     *
     * This supports nested arrays using flattened keys in a dot notation.
     *
     * @param string $key
     * @param array $value
     * @return int number of keys set
     */
    public function setHash(string $key, array $value): int
    {
        $value = self::flattenKeys($value);
        $ok = $this->hmSet($key, $value);
        if (!$ok) return 0;
        return count($value);
    }


    /**
     * Get an array from a hash.
     *
     * This supports nested arrays using flattened keys in a dot notation.
     *
     * @param string $key
     * @return array|null
     */
    public function getHash(string $key): ?array
    {
        $value = $this->hGetAll($key);
        if ($value === null) {
            return null;
        }

        $value = self::explodeFlatKeys($value);
        return $value;
    }


    /**
     * Store a value using the MessagePack format.
     *
     * @param string $key
     * @param mixed $value
     * @param int $ttl milliseconds
     * @return int
     */
    public function pack(string $key, $value, int $ttl = 0): int
    {
        $value = MessagePack::pack($value);
        $ok = $this->set($key, $value, $ttl);

        if (!$ok) {
            return 0;
        }

        return strlen($value);
    }


    /**
     * Get a value from the MessagePack format.
     *
     * @param string $key
     * @return mixed|null
     */
    public function unpack(string $key): mixed
    {
        $value = $this->get($key);

        if ($value === null) {
            return null;
        }

        return MessagePack::unpack($value);
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


    /**
     * Export to this file.
     *
     * - pattern (include pattern, default: '*')
     * - excludes (array of patterns)
     * - compressed (default true)
     * - log (callback)
     *
     * @param string|resource $file
     * @param array|string $config
     * @return int number of items exported
     * @throws Exception
     * @throws Exception fatal errors
     */
    public function export($file, $config = []): int
    {
        $export = new RdbExport($this, $config);
        return $export->export($file);
    }


    /**
     * Import from this file.
     *
     * - pattern (include pattern, default: '*')
     * - excludes (array of patterns)
     * - log (callback)
     *
     * @param string|resource $file
     * @param array|string $config
     * @return string[] errors
     * @throws Exception fatal errors
     */
    public function import($file, $config = []): array
    {
        $import = new RdbImport($this, $config);
        $import->import($file);
        return $import->errors;
    }
}
