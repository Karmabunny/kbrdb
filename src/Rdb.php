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
     * @return Generator<string|null>
     */
    public function mScan(array $keys): Generator
    {
        if (empty($keys)) return [];

        foreach (array_chunk($keys, $this->config->chunk_size) as $chunk) {
            $items = $this->mGet($chunk);
            foreach ($items as $item) yield $item;
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
        if ($expected and !class_exists($expected)) {
            throw new InvalidArgumentException('Not a class: ' . $expected);
        }

        $value = @unserialize($this->get($key));
        if ($value === false) return null;
        if (!is_object($value)) return null;

        if ($expected) {
            if (
                // Checker doesn't like string classes.
                // Or I'm dumb. One of those.
                // @phpstan-ignore-next-line
                get_class($value) !== $expected and
                !is_subclass_of($value, $expected, false)
            ) return null;
        }

        return $value;
    }


    /**
     * Bulk fetch objects via a list of keys.
     *
     * Empty keys are filtered out.
     *
     * @param string[] $keys Non-prefixed keys
     * @param string|null $expected Ensure all results inherits/is of this type
     * @return object[]
     * @throws InvalidArgumentException
     */
    public function mGetObjects(array $keys, string $expected = null): array
    {
        if ($expected and !class_exists($expected)) {
            throw new InvalidArgumentException('Not a class: ' . $expected);
        }

        // Fix sequential indexes.
        $keys = array_values($keys);

        if (empty($keys)) return [];

        $items = $this->mGet($keys);
        $output = [];

        foreach ($items as $index => $item) {
            $key = $keys[$index] ?? null;
            $item = @unserialize($item) ?: null;

            if (!$key or !$item) continue;

            if (!is_object($item)) continue;

            if (
                // @phpstan-ignore-next-line : Not sure about this one.
                get_class($item) !== $expected and
                !is_subclass_of($item, $expected, false)
            ) continue;

            $output[] = $item;
        }

        return $output;
    }


    /**
     * Bulk fetch objects via a list of keys.
     *
     * Empty keys are filtered out.
     *
     * @param iterable $keys Non-prefixed keys
     * @param string|null $expected Ensure all results is of this type
     * @return Generator<object>
     */
    public function mScanObjects($keys, string $expected = null): Generator
    {
        if ($expected and !class_exists($expected)) {
            throw new InvalidArgumentException('Not a class: ' . $expected);
        }

        $chunk = [];

        foreach ($keys as $key) {
            // Build a chunk of keys.
            $chunk[] = $key;
            if (count($chunk) !== $this->config->chunk_size) continue;

            // Fetch and yield them.
            $items = $this->mGetObjects($chunk, $expected);
            foreach ($items as $item) yield $item;
            $chunk = [];
        }

        // Also emit those leftovers.
        if (!empty($chunk)) {
            $items = $this->mGetObjects($chunk, $expected);
            foreach ($items as $item) yield $item;
        }
    }


    /**
     * Bulk set an array of object.
     *
     * @param object[] $items
     * @return int[] object sizes in bytes
     */
    public function mSetObjects(array $items): array
    {
        if (empty($items)) return [];

        $sizes = [];

        /** @var string[] $items */
        foreach ($items as &$item) {
            $item = serialize($item);
            $sizes[] = strlen($item);
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
