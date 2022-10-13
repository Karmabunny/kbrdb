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

        $retry_interval = $config->options['retry_interval'] ?? null;
        $retry_timeout = $config->options['retry_timeout'] ?? null;

        $success = $this->redis->connect(
            $config->getHost(),
            $config->getPort(),
            $config->timeout,
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

        // Skip empty results within the extension, which is hopefully
        // a bit faster than doing it in PHP.
        $this->redis->setOption(Redis::OPT_SCAN, Redis::SCAN_RETRY);
    }


    /**
     * Although php-redis is reasonably consistent with returning 'false'
     * for WRONGTYPE errors, that can't account for those boolean responses
     * that have a valid 'false' return. Lucky for us, they've stored that in
     * the 'script error' helper.
     *
     * @return bool
     */
    protected function hasWrongType(): bool
    {
        $error = $this->redis->getLastError();

        if ($error and strpos($error, 'WRONGTYPE') === 0) {
            $this->redis->clearLastError();
            return true;
        }

        return false;
    }


    /** @inheritdoc */
    public function registerSessionHandler(string $prefix = 'session:'): bool
    {
        ini_set('session.save_handler', 'redis');
        ini_set('session.save_path', vsprintf('%s?prefix=%s', [
            $this->config->getHost(true),
            $prefix,
        ]));

        // Assume it worked..?
        return true;
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
        // I can't trust that setOption(SCAN_PREFIX) is always available.
        $pattern = $this->config->prefix . $pattern;

        $it = null;

        for (;;) {
            $keys = $this->redis->scan($it, $pattern, $this->config->chunk_size);
            if ($keys === false) break;

            foreach ($keys as $key) {
                yield $this->stripPrefix($key);
            }
        }
    }


    /** @inheritdoc */
    public function ttl(string $key): ?int
    {
        $value = $this->redis->pttl($key);
        if ($value === false) return null;
        return $value;
    }


    /** @inheritdoc */
    public function expire(string $key, $ttl = 0): bool
    {
        return $this->redis->pExpire($key, $ttl);
    }


    /** @inheritdoc */
    public function expireAt(string $key, $ttl = 0): bool
    {
        return $this->redis->pExpireAt($key, $ttl);
    }


    /** @inheritdoc */
    public function rename(string $src, string $dst): bool
    {
        return $this->redis->rename($src, $dst);
    }


    /** @inheritdoc */
    public function type(string $key): ?string
    {
        $type = $this->redis->type($key);
        switch ($type) {
            case Redis::REDIS_STRING:
                return 'string';
            case Redis::REDIS_LIST:
                return 'list';
            case Redis::REDIS_SET:
                return 'set';
            case Redis::REDIS_ZSET:
                return 'zset';
            case Redis::REDIS_HASH:
                return 'hash';
        }
        return null;
    }


    /** @inheritdoc */
    public function set(string $key, string $value, int $ttl = 0, array $flags = [])
    {
        $flags = self::parseSetFlags($flags);

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
    public function get(string $key): ?string
    {
        $result = $this->redis->get($key);
        if ($result === false) return null;
        return $result;
    }


    /** @inheritdoc */
    public function mGet(iterable $keys): array
    {
        $keys = self::normalizeIterable($keys, false);

        if (empty($keys)) {
            return [];
        }

        $items = $this->redis->mGet($keys);

        foreach ($items as &$item) {
            if ($item !== false) continue;
            $item = null;
        }
        unset($item);

        $items = array_combine($keys, $items);
        return $items;
    }


    /** @inheritdoc */
    public function mSet(array $items): bool
    {
        if (empty($items)) return false;
        return $this->redis->mSet($items);
    }


    /** @inheritdoc */
    public function sAdd(string $key, ...$values): ?int
    {
        $values = self::flattenArrays($values);
        if (empty($values)) return 0;

        $res = $this->redis->sAdd($key, ...$values);

        // The docs are wrong - false is when it's 'not a set'.
        // Whereas 'value exists' is still an integer, because of being able to
        // add multiple values.
        if ($res === false) return null;

        return $res;
    }


    /** @inheritdoc */
    public function sMembers(string $key): ?array
    {
        /** @var array|false $items - typings are LYING */
        $items = $this->redis->sMembers($key);
        if ($items === false) return null;
        return $items;
    }


    /** @inheritdoc */
    public function sRem(string $key, ...$values): ?int
    {
        $values = self::flattenArrays($values);
        if (empty($values)) return 0;

        /** @var int|false $count */
        $count = $this->redis->sRem($key, ...$values);
        if ($count === false) return null;
        return $count;
    }


    /** @inheritdoc */
    public function sIsMember(string $key, string $value): ?bool
    {
        $ok = $this->redis->sIsMember($key, $value);
        if ($this->hasWrongType()) return null;
        return $ok;
    }


    /** @inheritdoc */
    public function sCard(string $key): ?int
    {
        /** @var int|false $ok */
        $ok = $this->redis->sCard($key);
        if ($ok === false) return null;
        return $ok;
    }


    /** @inheritdoc */
    public function sMove(string $src, string $dst, string $value): ?bool
    {
        $ok = $this->redis->sMove($src, $dst, $value);
        if ($this->hasWrongType()) return null;
        return $ok;
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
    public function lPush(string $key, ...$items): ?int
    {
        $count = $this->redis->lPush($key, ...$items);
        if ($count === false) return null;
        return $count;
    }


    /** @inheritdoc */
    public function rPush(string $key, ...$items): ?int
    {
        $count = $this->redis->rPush($key, ...$items);
        if ($count === false) return null;
        return $count;
    }


    /** @inheritdoc */
    public function lPop(string $key): ?string
    {
        /** @var mixed $value */
        $value = $this->redis->lPop($key);
        if ($value === false) return null;
        return $value;
    }


    /** @inheritdoc */
    public function rPop(string $key): ?string
    {
        /** @var mixed $value */
        $value = $this->redis->rPop($key);
        if ($value === false) return null;
        return $value;
    }


    /** @inheritdoc */
    public function rPoplPush(string $src, string $dst): ?string
    {
        /** @var string|false $value */
        $value = $this->redis->rPoplPush($src, $dst);
        if ($value === false) return null;
        return $value;
    }


    /** @inheritdoc */
    public function lRange(string $key, int $start = 0, int $stop = -1): ?array
    {
        /** @var array|false $range */
        $range = $this->redis->lRange($key, $start, $stop);
        if ($range === false) return null;
        return $range;
    }


    /** @inheritdoc */
    public function lTrim(string $key, int $start = 0, int $stop = -1): ?bool
    {
        $items = $this->redis->lTrim($key, $start, $stop);
        if ($items === false) return null;
        return (bool) $items;
    }


    /** @inheritdoc */
    public function lLen(string $key): ?int
    {
        $count = $this->redis->lLen($key);
        if ($count === false) return null;
        return $count;
    }


    /** @inheritdoc */
    public function lSet(string $key, int $index, string $item): ?bool
    {
        $ok = $this->redis->lSet($key, $index, $item);
        if (!$ok and $this->hasWrongType()) return null;
        return $ok;
    }


    /** @inheritdoc */
    public function lIndex(string $key, int $index): ?string
    {
        $item = $this->redis->lIndex($key, $index);
        if ($item === false) return null;
        return $item;
    }


    /** @inheritdoc */
    public function lRem(string $key, string $item, int $count = 0): ?int
    {
        // Args item/count are NOT swapped.
        $count = $this->redis->lRem($key, $item, $count);
        if ($count === false) return null;
        return $count;
    }


    /** @inheritdoc */
    public function blPop($keys, int $timeout = null): ?array
    {
        if (is_scalar($keys)) {
            $keys = [$keys];
        }
        else {
            $keys = self::normalizeIterable($keys, false);
        }

        if ($timeout === null) {
            $timeout = $this->config->timeout;
        }

        $value = $this->redis->blPop($keys, $timeout);
        if (empty($value)) return null;
        return $value;
    }


    /** @inheritdoc */
    public function brPop($keys, int $timeout = null): ?array
    {
        if (!is_scalar($keys)) {
            $keys = self::normalizeIterable($keys, false);
        }

        if ($timeout === null) {
            $timeout = $this->config->timeout;
        }

        $value = $this->redis->brPop($keys, $timeout);
        if (empty($value)) return null;
        return $value;
    }


    /** @inheritdoc */
    public function brPoplPush(string $src, string $dst, int $timeout = null): ?string
    {
        if ($timeout === null) {
            $timeout = $this->config->timeout;
        }

        $value = $this->redis->brpoplpush($src, $dst, $timeout);
        if ($value === false) return null;
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


    /** @inheritdoc */
    public function zAdd(string $key, array $members): int
    {
        $args = [];
        $args[] = $key;

        foreach ($members as $member => $score) {
            $args[] = $score;
            $args[] = $member;
        }

        return (int) @call_user_func_array([$this->redis, 'zAdd'], $args);
    }


    /** @inheritdoc */
    public function zIncrBy(string $key, float $value, string $member): float
    {
        return $this->redis->zIncrBy($key, $value, $member);
    }


    /** @inheritdoc */
    public function zRange(string $key, int $start = 0, int $stop = -1, bool $withscores = false): ?array
    {
        /** @var array|false $range */
        $range = $this->redis->zrange($key, $start, $stop, $withscores);
        if ($range === false) return null;
        return $range;
    }


    /** @inheritdoc */
    public function zRem(string $key, ...$members): int
    {
        $members = self::flattenArrays($members);
        return $this->redis->zRem($key, ...$members);
    }

}
