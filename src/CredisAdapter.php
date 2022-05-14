<?php
/**
 * @link      https://github.com/Karmabunny
 * @copyright Copyright (c) 2021 Karmabunny
 */

namespace karmabunny\rdb;

use Credis_Client;
use Generator;


/**
 * Rdb using the credis library, which conditionally wraps php-redis.
 *
 * @package karmabunny\rdb
 */
class CredisAdapter extends Rdb
{

    /** @var Credis_Client */
    public $credis;


    /** @inheritdoc */
    protected function __construct($config)
    {
        parent::__construct($config);
        $config = $this->config;

        $timeout = $config->options['timeout'] ?? null;
        $standalone = $config->options['standalone'] ?? false;

        $this->credis = new Credis_Client(
            $config->getHost(false),
            $config->getPort(),
            $timeout
        );

        if ($standalone) {
            $this->credis->forceStandalone();
        }

        $this->credis->connect();
    }


    /**
     * Credis can't guarantee that `setOption('prefix')` will always exist.
     *
     * So we're manually prefixing everything here. Yay!
     *
     * @param array $keys
     * @return string[]
     */
    protected function prefixKeys(array $keys): array
    {
        if (empty($keys)) return [];
        $keys = self::flattenArrays($keys);
        if (empty($keys)) return [];

        if ($this->config->prefix) {
            foreach ($keys as &$key) {
                $key = $this->config->prefix . $key;
            }
            unset($key);
        }

        return $keys;
    }


    /** @inheritdoc */
    public function keys(string $pattern): array
    {
        if ($this->config->scan_keys) {
            $keys = $this->scan($pattern);
            $keys = iterator_to_array($keys, false);
            return $keys;
        }

        $pattern = $this->config->prefix . $pattern;
        $keys = $this->credis->keys($pattern);
        return array_map([$this, 'stripPrefix'], $keys);
    }


    /** @inheritdoc */
    public function scan(string $pattern): Generator
    {
        $pattern = $this->config->prefix . $pattern;
        $it = null;

        for (;;) {
            $keys = $this->credis->scan($it, $pattern, $this->config->chunk_size);

            // If it's backed by php-redis it might return false.
            if ($keys) {
                foreach ($keys as $key) {
                    yield $this->stripPrefix($key);
                }
            }

            // The iterator is done.
            // Keys might not be empty though, so do this last.
            if (!$it) break;
        }
    }


    /** @inheritdoc */
    public function set(string $key, $value, $ttl = 0, $flags = [])
    {
        $flags = self::parseFlags($flags);

        $options = [];

        if ($ttl) {
            $name = $flags['time_at'] ? 'pxat' : 'px';
            $options[$name] = $ttl;
        }

        // Retain the TTL.
        if ($flags['keep_ttl']) {
            $options[] = 'keepttl';
        }

        // Toggle set-only flags.
        if ($flags['replace'] === true) {
            $options[] = 'xx';
        }
        else if ($flags['replace'] === false) {
            $options[] = 'nx';
        }

        // Get the value before setting it.
        if ($flags['get_set']) {
            $options[] = 'get';
        }

        $key = $this->config->prefix . $key;
        $result = $this->credis->set($key, $value, $options);

        // TODO Uhh.. does getset actually work here?
        if ($flags['get_set']) {
            return $result === false ? null : $result;
        }

        return (bool) $result;
    }


    /** @inheritdoc */
    public function get(string $key)
    {
        $key = $this->config->prefix . $key;
        $result = $this->credis->get($key);
        if (!is_string($result)) return null;
        return $result;
    }


    /** @inheritdoc */
    public function mGet(array $keys): array
    {
        $keys = $this->prefixKeys($keys);
        if (empty($keys)) return [];

        $items = $this->credis->mGet($keys);
        foreach ($items as &$item) {
            if ($item !== false) continue;
            $item = null;
        }
        unset($item);
        return $items;
    }


    /** @inheritdoc */
    public function mSet(array $items): bool
    {
        if (empty($items)) return false;

        $keys = array_keys($items);
        $keys = $this->prefixKeys($keys);
        $items = array_combine($keys, $items);

        return (bool) @$this->credis->mSet($items);
    }


    /** @inheritdoc */
    public function sAdd(string $key, ...$values): int
    {
        if (empty($values)) return 0;
        $key = $this->config->prefix . $key;
        return $this->credis->sAdd($key, ...$values);
    }


    /** @inheritdoc */
    public function sMembers(string $key): array
    {
        $key = $this->config->prefix . $key;
        return $this->credis->sMembers($key);
    }


    /** @inheritdoc */
    public function sRem(string $key, ...$values): int
    {
        if (empty($values)) return 0;
        $key = $this->config->prefix . $key;
        return $this->credis->sRem($key, $values);
    }


    /** @inheritdoc */
    public function incr(string $key, int $amount = 1): int
    {
        $key = $this->config->prefix . $key;
        return $this->credis->incrBy($key, $amount);
    }


    /** @inheritdoc */
    public function decr(string $key, int $amount = 1): int
    {
        $key = $this->config->prefix . $key;
        return $this->credis->decrBy($key, $amount);
    }


    /** @inheritdoc */
    public function lPush(string $key, ...$items)
    {
        $key = $this->config->prefix . $key;
        $count = $this->credis->lPush($key, ...$items);
        if (!is_numeric($count)) return null;
        return $count;
    }


    /** @inheritdoc */
    public function rPush(string $key, ...$items)
    {
        $key = $this->config->prefix . $key;
        $count = $this->credis->rPush($key, ...$items);
        if (!is_numeric($count)) return null;
        return $count;
    }


    /** @inheritdoc */
    public function lPop(string $key)
    {
        $key = $this->config->prefix . $key;
        return $this->credis->lPop($key);
    }


    /** @inheritdoc */
    public function rPop(string $key)
    {
        $key = $this->config->prefix . $key;
        return $this->credis->rPop($key);
    }


    /** @inheritdoc */
    public function lRange(string $key, int $start = 0, int $stop = -1): array
    {
        $key = $this->config->prefix . $key;
        return $this->credis->lRange($key, $start, $stop);
    }


    /** @inheritdoc */
    public function lTrim(string $key, int $start = 0, int $stop = -1): bool
    {
        $key = $this->config->prefix . $key;
        return $this->credis->lTrim($key, $start, $stop);
    }


    /** @inheritdoc */
    public function lLen(string $key)
    {
        $key = $this->config->prefix . $key;
        $count = $this->credis->lLen($key);
        if (!is_numeric($count)) return null;
        return $count;
    }


    /** @inheritdoc */
    public function lSet(string $key, int $index, string $item): bool
    {
        $key = $this->config->prefix . $key;
        return $this->credis->lSet($key, $index, $item);
    }


    /** @inheritdoc */
    public function lIndex(string $key, int $index)
    {
        $key = $this->config->prefix . $key;
        return $this->credis->lIndex($key, $index);
    }


    /** @inheritdoc */
    public function lRem(string $key, string $item, int $count = 0): int
    {
        // Args item/count are swapped.
        $key = $this->config->prefix . $key;
        return $this->credis->lRem($key, $count, $item);
    }


    /** @inheritdoc */
    public function blPop($keys, int $timeout = 0)
    {
        if (!is_array($keys)) {
            $keys = [$keys];
        }

        $keys = $this->prefixKeys($keys);

        // I can only assume the typings are lying.
        $args = $keys;
        $args[] = $timeout;

        $value = $this->credis->__call('blPop', $args);
        if (empty($value)) return null;
        return $value;
    }


    /** @inheritdoc */
    public function brPop($keys, int $timeout = 0)
    {
        if (!is_array($keys)) {
            $keys = [$keys];
        }

        $keys = $this->prefixKeys($keys);

        // I can only assume the typings are lying.
        $args = $keys;
        $args[] = $timeout;

        $value = $this->credis->__call('brPop', $args);
        if (empty($value)) return null;
        return $value;
    }


    /** @inheritdoc */
    public function brPoplPush(string $src, string $dst, int $timeout = 0)
    {
        $src = $this->config->prefix . $src;
        $dst = $this->config->prefix . $dst;

        $items = $this->credis->brPoplPush($src, $dst, $timeout);

        if (empty($items)) return null;
        return reset($items);
    }


    /** @inheritdoc */
    public function exists(...$keys): int
    {
        $keys = $this->prefixKeys($keys);
        if (empty($keys)) return 0;

        return $this->credis->exists(...$keys);
    }


    /** @inheritdoc */
    public function del(...$keys): int
    {
        $keys = $this->prefixKeys($keys);
        if (empty($keys)) return 0;

        return $this->credis->del(...$keys);
    }

}
