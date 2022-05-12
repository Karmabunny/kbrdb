<?php
/**
 * @link      https://github.com/Karmabunny
 * @copyright Copyright (c) 2021 Karmabunny
 */

namespace karmabunny\rdb;

use Generator;
use Redis;
use RedisException;


/**
 * Rdb using the php-redis binary extension.
 *
 * @package karmabunny\rdb
 */
class PhpRedisAdapter extends Rdb
{

    /** @var Redis */
    private $redis;


    /** @inheritdoc */
    protected function __construct($config)
    {
        parent::__construct($config);
        $config = $this->config;

        $this->redis = new Redis();

        $timeout = $config->options['timeout'] ?? null;
        $retry_interval = $config->options['retry_interval'] ?? null;
        $retry_timeout = $config->options['retry_timeout'] ?? null;

        $success = $this->redis->connect(
            $config->getHost(),
            $config->getPort(),
            $timeout,
            null,
            $retry_interval,
            $retry_timeout
        );

        if (!$success) {
            throw new RedisException('Unable to connect to Redis server: ' . $config->host);
        }

        if ($config->prefix) {
            $this->redis->setOption(Redis::OPT_PREFIX, $config->prefix);
        }
    }


    /** @inheritdoc */
    public function keys(string $pattern): array
    {
        if ($this->config->scan_keys) {
            $keys = $this->scan($pattern);
            $keys = iterator_to_array($keys, false);
            return $keys;
        }

        $keys = $this->redis->keys($pattern);
        return array_map([$this, 'stripPrefix'], $keys);
    }


    /** @inheritdoc */
    public function scan(string $pattern): Generator
    {
        $it = null;

        while ($keys = $this->redis->scan($it, $pattern, $this->config->chunk_size)) {
            foreach ($keys as $key) {
                yield $this->stripPrefix($key);
            }
        }
    }


    /** @inheritdoc */
    public function set(string $key, string $value, $ttl = 0, $flags = [])
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

        $result = $this->redis->set($key, $value, $options);

        // TODO Uhh.. does getset actually work here?
        if ($flags['get_set']) {
            return $result === false ? null : $result;
        }

        return (bool) $result;
    }


    /** @inheritdoc */
    public function get(string $key)
    {
        $result = $this->redis->get($key);
        if ($result === false) return null;
        return $result;
    }


    /** @inheritdoc */
    public function mGet(array $keys): array
    {
        if (empty($keys)) return [];
        $items = $this->redis->mGet($keys);

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
        return $this->redis->mSet($items);
    }


    /** @inheritdoc */
    public function sAdd(string $key, ...$values): int
    {
        if (empty($values)) return 0;
        $values = self::flattenArrays($values);
        return $this->redis->sAdd($key, ...$values);
    }


    /** @inheritdoc */
    public function sMembers(string $key): array
    {
        $items = $this->redis->sMembers($key);
        if ($items === false) return [];
        return $items;
    }


    /** @inheritdoc */
    public function sRem(string $key, ...$values): int
    {
        if (empty($values)) return 0;
        $values = self::flattenArrays($values);
        return $this->redis->sRem($key, ...$values);
    }


    /** @inheritdoc */
    public function incr(string $key, int $amount = 1): int
    {
        return $this->redis->incrby($key, $amount);
    }


    /** @inheritdoc */
    public function decr(string $key, int $amount = 1): int
    {
        return $this->redis->decrby($key, $amount);
    }


    /** @inheritdoc */
    public function lPush(string $key, ...$items)
    {
        $count = $this->redis->lPush($key, ...$items);
        if ($count === false) return null;
        return $count;
    }


    /** @inheritdoc */
    public function rPush(string $key, ...$items)
    {
        $count = $this->redis->rPush($key, ...$items);
        if ($count === false) return null;
        return $count;
    }


    /** @inheritdoc */
    public function lPop(string $key)
    {
        $value = $this->redis->lPop($key);
        if ($value === false) return null;
        return $value;
    }


    /** @inheritdoc */
    public function rPop(string $key)
    {
        $value = $this->redis->rPop($key);
        if ($value === false) return null;
        return $value;
    }


    /** @inheritdoc */
    public function lRange(string $key, int $start = 0, int $stop = -1): array
    {
        return $this->redis->lRange($key, $start, $stop);
    }


    /** @inheritdoc */
    public function lTrim(string $key, int $start = 0, int $stop = -1): bool
    {
        return $this->redis->lTrim($key, $start, $stop);
    }


    /** @inheritdoc */
    public function lLen(string $key)
    {
        $count = $this->redis->lLen($key);
        if ($count === false) return null;
        return $count;
    }


    /** @inheritdoc */
    public function lSet(string $key, int $index, string $item): bool
    {
        return $this->redis->lSet($key, $index, $item);
    }


    /** @inheritdoc */
    public function lIndex(string $key, int $index)
    {
        $item = $this->redis->lIndex($key, $index);
        if ($item === false) return null;
        return $item;
    }


    /** @inheritdoc */
    public function lRem(string $key, string $item, int $count = 0): int
    {
        return $this->redis->lRem($key, $item, $count);
    }


    /** @inheritdoc */
    public function blPop($keys, int $timeout = 0)
    {
        if (!is_array($keys)) {
            $keys = [$keys];
        }

        $value = $this->redis->blPop($keys, $timeout);
        if (empty($value)) return null;
        return $value;
    }


    /** @inheritdoc */
    public function brPop($keys, int $timeout = 0)
    {
        if (!is_array($keys)) {
            $keys = [$keys];
        }

        $value = $this->redis->brPop($keys, $timeout);
        if (empty($value)) return null;
        return $value;
    }


    /** @inheritdoc */
    public function exists(...$keys): int
    {
        $keys = self::flattenArrays($keys);
        if (empty($keys)) return 0;
        return $this->redis->exists($keys);
    }


    /** @inheritdoc */
    public function del(...$keys): int
    {
        $keys = self::flattenArrays($keys);
        if (empty($keys)) return 0;
        return $this->redis->del($keys);
    }

}
