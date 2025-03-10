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
 * Adapter options:
 * - `retry_interval`
 * - `retry_timeout`
 * - `use_native_session`
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

        $retry_interval = $config->options['retry_interval'] ?? 0;
        $retry_timeout = $config->options['retry_timeout'] ?? 0;

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

        $this->redis->select($this->config->database);
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
    public function flushAll(bool $async = false)
    {
        $this->redis->flushAll($async);
    }


    /** @inheritdoc */
    public function flushDb(bool $async = false)
    {
        $this->redis->flushDB($async);
    }


    /** @inheritdoc */
    public function select(int $database): bool
    {
        return (bool) $this->redis->select($database);
    }


    /** @inheritdoc */
    public function move(string $key, int $database): bool
    {
        return (bool) $this->redis->move($key, $database);
    }


    /** @inheritdoc */
    public function registerSessionHandler(string $prefix = 'session:'): bool
    {
        if (!empty($this->config->options['use_native_session'])) {
            ini_set('session.save_handler', 'redis');
            ini_set('session.save_path', vsprintf('%s?prefix=%s', [
                $this->config->getHost(true),
                $this->config->prefix . $prefix,
            ]));

            // Assume it worked..?
            return true;
        }
        else {
            return parent::registerSessionHandler($prefix);
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
        // I can't trust that setOption(SCAN_PREFIX) is always available.
        $pattern = $this->config->prefix . $pattern;

        $it = null;

        for (;;) {
            $keys = $this->redis->scan($it, $pattern, $this->config->scan_size);
            if ($keys === false) break;

            foreach ($keys as $key) {
                yield $this->stripPrefix($key);
            }

            // The iterator is done.
            // Keys might not be empty though, so do this last.
            if (!$it) break;
        }
    }


    /** @inheritdoc */
    public function dump(string $key): ?string
    {
        return $this->redis->dump($key) ?: null;
    }


    /** @inheritdoc */
    public function restore(string $key, int $ttl, string $value, array $flags = []): bool
    {
        $flags = $this->parseRestoreFlags($flags);

        if ($flags['replace']) {
            $this->del($key);
        }

        $ok = $this->redis->restore($key, $ttl, $value);
        return $ok !== false;
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
    public function append(string $key, string $value): int
    {
        return (int) $this->redis->append($key, $value);
    }


    /** @inheritdoc */
    public function get(string $key): ?string
    {
        $result = $this->redis->get($key);
        if ($result === false) return null;
        return $result;
    }


    /** @inheritdoc */
    public function getRange(string $key, int $from = 0, int $to = -1): ?string
    {
        /** @var string|false $result */
        $result = $this->redis->getRange($key, $from, $to);
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
        if ($this->config->scan_keys) {
            $items = $this->sScan($key);
            return iterator_to_array($items, false);
        }

        /** @var array|false $items - typings are LYING */
        $items = $this->redis->sMembers($key);
        if ($items === false) return null;
        return $items;
    }


    /** @inheritdoc */
    public function sScan(string $key, ?string $pattern = null): Generator
    {
        $pattern = $pattern ?: '*';
        $it = null;

        for (;;) {
            $items = $this->redis->sscan($key, $it, $pattern, $this->config->scan_size);
            if ($items === false) break;

            foreach ($items as $item) {
                yield $item;
            }

            // The iterator is done.
            // Keys might not be empty though, so do this last.
            if (!$it) break;
        }
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
    public function incrBy(string $key, int $amount): int
    {
        return $this->redis->incrby($key, $amount);
    }


    /** @inheritdoc */
    public function incrByFloat(string $key, float $amount): float
    {
        return (float) $this->redis->incrbyfloat($key, $amount);
    }


    /** @inheritdoc */
    public function decrBy(string $key, int $amount): int
    {
        return $this->redis->decrby($key, $amount);
    }


    /** @inheritdoc */
    public function lPush(string $key, ...$items): ?int
    {
        $items = $this->flatten($items, 2);
        $count = $this->redis->lPush($key, ...$items);
        if ($count === false) return null;
        return $count;
    }


    /** @inheritdoc */
    public function rPush(string $key, ...$items): ?int
    {
        $items = $this->flatten($items, 2);
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
    public function blPop($keys, ?int $timeout = null): ?array
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
    public function brPop($keys, ?int $timeout = null): ?array
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
    public function brPoplPush(string $src, string $dst, ?int $timeout = null): ?string
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
    public function zRange(string $key, $start = null, $stop = null, array $flags = []): ?array
    {
        $flags = self::parseRangeFlags($flags);

        if ($flags['limit']) {
            $limit = [ $flags['offset'], $flags['count'] ];
        }
        else {
            $limit = null;
        }

        if ($flags['rev']) {
            if ($flags['byscore']) {
                $start = $start ?? '-inf';
                $stop = $stop ?? '+inf';

                $range = $this->redis->zRevRangeByScore($key, $start, $stop, [
                    'withscores' => $flags['withscores'],
                    'limit' => $limit,
                ]);
            }
            else if ($flags['bylex']) {

                if ($start and !preg_match('/^\[|^\(/', $start)) {
                    $start = '[' . $start;
                }
                if ($stop and !preg_match('/^\[|^\(/', $stop)) {
                    $stop = '[' . $stop;
                }
                $start = $start ?? '-';
                $stop = $stop ?? '+';

                if ($limit) {
                    [$offset, $count] = $limit;
                    $range = $this->redis->zRevRangeByLex($key, $start, $stop, $offset, $count);
                }
                else {
                    $range = $this->redis->zRevRangeByLex($key, $start, $stop);
                }
            }
            else {
                $start = $start ?? 0;
                $stop = $stop ?? -1;

                $range = $this->redis->zRevRange($key, $start, $stop, $flags['withscores']);
            }
        }
        else {
            if ($flags['byscore']) {
                $start = $start ?? '-inf';
                $stop = $stop ?? '+inf';

                $range = $this->redis->zRangeByScore($key, $start, $stop, [
                    'withscores' => $flags['withscores'],
                    'limit' => $limit,
                ]);
            }
            else if ($flags['bylex']) {

                if ($start and !preg_match('/^\[|^\(/', $start)) {
                    $start = '[' . $start;
                }
                if ($stop and !preg_match('/^\[|^\(/', $stop)) {
                    $stop = '[' . $stop;
                }

                $start = $start ?? '-';
                $stop = $stop ?? '+';

                if ($limit) {
                    [$offset, $count] = $limit;
                    $range = $this->redis->zRangeByLex($key, $start, $stop, $offset, $count);
                }
                else {
                    $range = $this->redis->zRangeByLex($key, $start, $stop);
                }
            }
            else {
                $start = $start ?? 0;
                $stop = $stop ?? -1;

                $range = $this->redis->zrange($key, $start, $stop, $flags['withscores']);
            }
        }

        if ($range === false) return null;
        return $range;
    }


    /** @inheritdoc */
    public function zRem(string $key, ...$members): int
    {
        $members = self::flattenArrays($members);
        return $this->redis->zRem($key, ...$members);
    }


    /** @inheritdoc */
    public function zCard(string $key): ?int
    {
        /** @var int|false $res */
        $res = $this->redis->zCard($key);
        if ($res === false) return null;
        return $res;
    }


    /** @inheritdoc */
    public function zCount(string $key, float $min, float $max): ?int
    {
        /** @var int|false $res */
        // @phpstan-ignore-next-line
        $res = $this->redis->zCount($key, $min, $max);
        if ($res === false) return null;
        return $res;
    }


    /** @inheritdoc */
    public function zScore(string $key, string $member): ?float
    {
        $res = $this->redis->zScore($key, $member);
        if ($res === false) return null;
        return $res;
    }


    /** @inheritdoc */
    public function zRank(string $key, string $member): ?int
    {
        /** @var int|false $res */
        $res = $this->redis->zRank($key, $member);
        if ($res === false) return null;
        return $res;
    }


    /** @inheritdoc */
    public function zRevRank(string $key, string $member): ?int
    {
        /** @var int|false $ok */
        $ok = $this->redis->zRevRank($key, $member);
        if ($ok === false) return null;
        return $ok;
    }


    /** @inheritdoc */
    public function hDel(string $key, ...$fields): int
    {
        $fields = self::flatten($fields);
        return $this->redis->hDel($key, ...$fields);
    }


    /** @inheritdoc */
    public function hExists(string $key, string $field): bool
    {
        return (bool) $this->redis->hExists($key, $field);
    }


    /** @inheritdoc */
    public function hSet(string $key, string $field, $value, bool $replace = true): bool
    {
        if ($replace) {
            $ok = $this->redis->hSet($key, $field, $value);
        }
        else {
            $ok = $this->redis->hSetNx($key, $field, $value);
        }
        return (bool) $ok;
    }


    /** @inheritdoc */
    public function hGet(string $key, string $field): ?string
    {
        $res = $this->redis->hGet($key, $field);
        if ($res === false) return null;
        return $res;
    }


    /** @inheritdoc */
    public function hGetAll(string $key): ?array
    {
        $res = $this->redis->hGetAll($key);
        if (empty($res)) return null;
        return $res;
    }


    /** @inheritdoc */
    public function hIncrBy(string $key, string $field, int $amount): int
    {
        $res = $this->redis->hIncrBy($key, $field, $amount);
        return (int) $res;
    }


    /** @inheritdoc */
    public function hIncrByFloat(string $key, string $field, float $amount): float
    {
        $res = $this->redis->hIncrByFloat($key, $field, $amount);
        return (float) $res;
    }


    /** @inheritdoc */
    public function hStrLen(string $key, string $field): ?int
    {
        $res = $this->redis->hStrLen($key, $field);
        if ($res === false) return null;
        return (int) $res;
    }


    /** @inheritdoc */
    public function hKeys(string $key): ?array
    {
        $res = $this->redis->hKeys($key);
        if (!is_array($res) or empty($res)) return null;
        return $res;
    }


    /** @inheritdoc */
    public function hVals(string $key): ?array
    {
        $res = $this->redis->hVals($key);
        if (!is_array($res) or empty($res)) return null;
        return $res;
    }


    /** @inheritdoc */
    public function hLen(string $key): int
    {
        $res = $this->redis->hLen($key);
        if (!is_numeric($res)) return 0;
        return (int) $res;
    }


    /** @inheritdoc */
    public function hmGet(string $key, ...$fields): ?array
    {
        $values = $this->redis->hMGet($key, $fields);
        if ($values === false) return null;
        return array_values($values);
    }


    /** @inheritdoc */
    public function hmSet(string $key, array $fields): bool
    {
        $res = $this->redis->hmSet($key, $fields);
        return (bool) $res;
    }


    /** @inheritdoc */
    public function hScan(string $key, ?string $pattern = null): Generator
    {
        $pattern = $pattern ?: '*';
        $it = null;

        for (;;) {
            $items = $this->redis->hScan($key, $it, $pattern, $this->config->scan_size);
            if ($items === false) break;

            foreach ($items as $key => $value) {
                yield $key => $value;
            }

            // The iterator is done.
            // Keys might not be empty though, so do this last.
            if (!$it) break;
        }
    }

}
