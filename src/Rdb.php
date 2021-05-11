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
 * It doesn't implement connection utilities at all, it simply provides a
 * unified interface for different adapters (php-redis, predis).
 *
 * It also contains some useful helpers, mostly around object serialisation.
 * Feel free to add to it.
 *
 * @package karmabunny\rdb
 */
abstract class Rdb
{
    const ADAPTERS = [
        RdbConfig::TYPE_PHP_REDIS => PhpRedisAdapter::class,
        RdbConfig::TYPE_PREDIS => PredisAdapter::class,
    ];


    /** @var RdbConfig */
    public $config;


    /**
     * - host
     * - prefix
     * - other predis options
     *
     * @example
     *   new RmsCache([
     *     'host' => '127.0.0.1:6379',
     *     'prefix' => 'etc:rms:',
     *   ]);
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
     *
     * @param RdbConfig|array $config
     * @return Rdb
     */
    public static function create($config): Rdb
    {
        if (is_array($config)) {
            $config = new RdbConfig($config);
        }

        $adapter = self::ADAPTERS[$config->adapter] ?? PredisAdapter::class;
        return new $adapter($config);
    }


    /**
     * Strip the prefix from list of keys.
     *
     * @param string $keys
     * @return string[]
     */
    protected function stripPrefix(...$keys): array
    {
        if ($this->config->prefix) {
            foreach ($keys as &$key) {
                $key = preg_replace("/^{$this->config->prefix}/", '', $key);
            }
        }

        return $keys;
    }


    /**
     * Flatten an array input.
     *
     * @param array $items
     * @return array
     */
    protected static function expandArrays(array $items): array
    {
        $output = [];
        array_walk_recursive($items, function($item) use (&$output) {
            $output[] = $item;
        });
        return $output;
    }


    /**
     *
     * @param string $key
     * @param string $value
     * @param int $ttl milliseconds
     * @return bool
     */
    public abstract function set(string $key, string $value, $ttl = 0): bool;


    /**
     *
     * @param string $key
     * @return string|null
     */
    public abstract function get(string $key): ?string;


    /**
     *
     * @param string[] $keys
     * @return string[]
     */
    public abstract function mGet(array $keys): array;

    /**
     *
     * @param string[] $items key => string
     * @return bool
     */
    public abstract function mSet(array $items): bool;


    /**
     *
     * @param string $key
     * @param mixed $values
     * @return int
     */
    public abstract function sAdd(string $key, ...$values): int;


    /**
     *
     * @param string $key
     * @return array
     */
    public abstract function sMembers(string $key): array;


    /**
     *
     * @param string|string[] $keys
     * @return int
     */
    public abstract function exists(...$keys): int;


    /**
     *
     * @param string|string[] $keys
     * @return int
     */
    public abstract function del(...$keys): int;


    /**
     *
     * @param string $pattern
     * @return string[]
     */
    public abstract function keys(string $pattern): array;


    /**
     *
     * @param string $pattern
     * @return Generator
     */
    public abstract function scan(string $pattern): Generator;



    /**
     * Bulk fetch via a list of keys.
     *
     * Empty keys are filtered out.
     *
     * @param array $keys
     * @return Generator
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
     *
     * @param string $key
     * @param object $value
     * @return int
     */
    public function setObject(string $key, $value): int
    {
        $value = serialize($value);
        if (!$this->set($key, $value)) return 0;
        return strlen($value);
    }


    /**
     *
     * @param string $key
     * @param string|null $expected
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
     * @param string|null $expected Ensure all results is of this type
     * @return object[]
     * @throws InvalidArgumentException
     */
    public function mGetObjects(array $keys, string $expected = null): array
    {
        if ($expected and !class_exists($expected)) {
            throw new InvalidArgumentException('Not a class: ' . $expected);
        }

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
     * @param array $keys
     * @param string|null $expected Ensure all results is of this type
     * @return Generator<object>
     */
    public function mScanObjects(array $keys, string $expected = null): Generator
    {
        if ($expected and !class_exists($expected)) {
            throw new InvalidArgumentException('Not a class: ' . $expected);
        }

        if (empty($keys)) return [];

        foreach (array_chunk($keys, $this->config->chunk_size) as $chunk) {
            $items = $this->mGetObjects($chunk, $expected);
            foreach ($items as $item) yield $item;
        }
    }


    /**
     *
     * @param object[] $items
     * @return int[]
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
     *
     * @param string $key
     * @param array|JsonSerializable $value
     * @return int
     */
    public function setJson(string $key, $value): int
    {
        $value = json_encode($value);
        if (!$this->set($key, $value)) return 0;
        return strlen($value);
    }


    /**
     *
     * @param string $key
     * @return array
     */
    public function getJson(string $key): array
    {
        $out = json_decode($this->get($key), true);

        $error = json_last_error();
        if ($error !== JSON_ERROR_NONE) {
            throw new JsonException(json_last_error_msg(), $error);
        }

        return $out;
    }

}
