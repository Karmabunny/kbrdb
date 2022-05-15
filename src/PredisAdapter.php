<?php
/**
 * @link      https://github.com/Karmabunny
 * @copyright Copyright (c) 2021 Karmabunny
 */

namespace karmabunny\rdb;

use Generator;
use Predis\Client;
use Predis\Collection\Iterator\Keyspace;
use Predis\Response\ServerException;
use Predis\Response\Status;

/**
 * Rdb using the predis library.
 *
 * @package karmabunny\rdb
 */
class PredisAdapter extends Rdb
{

    /** @var Client */
    public $predis;


    /** @inheritdoc */
    protected function __construct($config)
    {
        parent::__construct($config);
        $config = $this->config;

        $options = $config->options;
        $options['prefix'] = $config->prefix;
        $options['timeout'] = $config->timeout;

        $this->predis = new Client($config->getHost(true), $options);
        $this->predis->connect();
    }


    /** @inheritdoc */
    public function keys(string $pattern): array
    {
        if ($this->config->scan_keys) {
            $keys = $this->scan($pattern);
            $keys = iterator_to_array($keys, false);
            return $keys;
        }

        $keys = $this->predis->keys($pattern);
        return array_map([$this, 'stripPrefix'], $keys);
    }


    /** @inheritdoc */
    public function scan(string $pattern): Generator
    {
        $pattern = $this->config->prefix . $pattern;
        $iterator = new Keyspace($this->predis, $pattern, $this->config->chunk_size);

        foreach ($iterator as $key) {
            $key = $this->stripPrefix($key);
            yield $key;
        }
    }


    /** @inheritdoc */
    public function ttl(string $key): ?int
    {
        return $this->predis->ttl($key);
    }


    /** @inheritdoc */
    public function expire(string $key, $ttl = 0): bool
    {
        return (bool) $this->predis->pexpire($key, $ttl);
    }


    /** @inheritdoc */
    public function expireAt(string $key, $ttl = 0): bool
    {
        return (bool) $this->predis->pexpireat($key, $ttl);
    }


    /** @inheritdoc */
    public function rename(string $src, string $dst): bool
    {
        return (bool) $this->predis->rename($src, $dst);
    }


    /** @inheritdoc */
    public function type(string $key): ?string
    {
        $type = $this->predis->type($key);
        if ($type === 'none') return null;
        return $type;
    }


    /** @inheritdoc */
    public function set(string $key, $value, $ttl = 0, $flags = [])
    {
        $flags = self::parseFlags($flags);

        $args = [];
        $args[] = $key;
        $args[] = $value;

        if ($ttl) {
            $args[] = $flags['time_at'] ? 'PXAT' : 'PX';
            $args[] = $ttl;
        }

        // Retain the TTL.
        if ($flags['keep_ttl']) {
            $args[] = 'KEEPTTL';
        }

        // Toggle set-only flags.
        if ($flags['replace'] === true) {
            $args[] = 'XX';
        }
        else if ($flags['replace'] === false) {
            $args[] = 'NX';
        }

        // Get the value before setting it.
        if ($flags['get_set']) {
            $args[] = 'GET';
        }

        /** @var Status */
        $status = @call_user_func_array([$this->predis, 'set'], $args);

        if ($flags['get_set']) {
            return $status->getPayload();
        }

        return $status == 'OK';
    }


    /** @inheritdoc */
    public function get(string $key): ?string
    {
        return $this->predis->get($key);
    }


    /** @inheritdoc */
    public function mGet(iterable $keys): array
    {
        $keys = self::normalizeIterable($keys, false);

        if (empty($keys)) {
            return [];
        }

        $items = $this->predis->mget($keys);
        $items = array_combine($keys, $items);
        return $items;
    }


    /** @inheritdoc */
    public function mSet(array $items): bool
    {
        if (empty($items)) return false;
        return (bool) @$this->predis->mset($items);
    }


    /** @inheritdoc */
    public function sAdd(string $key, ...$values): int
    {
        $values = self::flattenArrays($values);
        if (empty($values)) return 0;

        return $this->predis->sadd($key, $values);
    }


    /** @inheritdoc */
    public function sMembers(string $key): array
    {
        return $this->predis->smembers($key);
    }


    /** @inheritdoc */
    public function sRem(string $key, ...$values): int
    {
        $values = self::flattenArrays($values);
        if (empty($values)) return 0;

        return $this->predis->srem($key, $values);
    }


    /** @inheritdoc */
    public function sIsMember(string $key, string $value): bool
    {
        $ok = (bool) $this->predis->sismember($key, $value);
        return $ok;
    }


    /** @inheritdoc */
    public function sCard(string $key): int
    {
        return $this->predis->scard($key);
    }


    /** @inheritdoc */
    public function sMove(string $src, string $dst, string $value): bool
    {
        $ok = (bool) $this->predis->smove($src, $dst, $value);
        return $ok;
    }



    /** @inheritdoc */
    public function incr(string $key, int $amount = 1): int
    {
        return $this->predis->incrby($key, $amount);
    }


    /** @inheritdoc */
    public function decr(string $key, int $amount = 1): int
    {
        return $this->predis->decrby($key, $amount);
    }


    /** @inheritdoc */
    public function lPush(string $key, ...$items): ?int
    {
        $count = $this->predis->lpush($key, $items);
        if ($count <= 0) return null;
        return $count;
    }


    /** @inheritdoc */
    public function rPush(string $key, ...$items): ?int
    {
        $count = $this->predis->rpush($key, $items);
        if ($count <= 0) return null;
        return $count;
    }


    /** @inheritdoc */
    public function lPop(string $key): ?string
    {
        return $this->predis->lpop($key);
    }


    /** @inheritdoc */
    public function rPop(string $key): ?string
    {
        return $this->predis->rpop($key);
    }


    /** @inheritdoc */
    public function rPoplPush(string $src, string $dst): ?string
    {
        return $this->predis->rpoplpush($src, $dst);
    }


    /** @inheritdoc */
    public function lRange(string $key, int $start = 0, int $stop = -1): array
    {
        try {
            $range = $this->predis->lrange($key, $start, $stop);
            return $range;
        }
        catch (ServerException $exception) {
            if (strpos($exception->getMessage(), 'WRONGTYPE') === 0) {
                return [];
            }
            throw $exception;
        }
    }


    /** @inheritdoc */
    public function lTrim(string $key, int $start = 0, int $stop = -1): bool
    {
        return (bool) $this->predis->ltrim($key, $start, $stop);
    }


    /** @inheritdoc */
    public function lLen(string $key): ?int
    {
        $count = $this->predis->llen($key);
        if ($count < 0) return null;
        return $count;
    }


    /** @inheritdoc */
    public function lSet(string $key, int $index, string $item): bool
    {
        return (bool) $this->predis->lset($key, $index, $item);
    }


    /** @inheritdoc */
    public function lIndex(string $key, int $index): ?string
    {
        return $this->predis->lindex($key, $index);
    }


    /** @inheritdoc */
    public function lRem(string $key, string $item, int $count = 0): int
    {
        // Args item/count are swapped.
        return $this->predis->lrem($key, $count, $item);
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

        return $this->predis->blpop($keys, $timeout);
    }


    /** @inheritdoc */
    public function brPop($keys, int $timeout = null): ?array
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

        return $this->predis->brpop($keys, $timeout);
    }


    /** @inheritdoc */
    public function brPoplPush(string $src, string $dst, int $timeout = null): ?string
    {
        if ($timeout === null) {
            $timeout = $this->config->timeout;
        }

        return $this->predis->brpoplpush($src, $dst, $timeout);
    }


    /** @inheritdoc */
    public function exists(...$keys): int
    {
        $keys = self::flattenArrays($keys);
        if (empty($keys)) return 0;

        return $this->predis->exists(...$keys);
    }


    /** @inheritdoc */
    public function del(...$keys): int
    {
        $keys = self::flattenArrays($keys);
        if (empty($keys)) return 0;

        return $this->predis->del($keys);
    }

}
