<?php
/**
 * @link      https://github.com/Karmabunny
 * @copyright Copyright (c) 2021 Karmabunny
 */

namespace karmabunny\rdb;

use Generator;
use karmabunny\rdb\Wrappers\Credis;

/**
 * Rdb using the credis library, which conditionally wraps php-redis.
 *
 * Adapter options:
 * - `standalone` : force non-binary mode
 * - `use_native_session` : prefer use of the native session handler
 *
 * Note, the 'native session' handler is only available in binary mode.
 *
 * @package karmabunny\rdb
 */
class CredisAdapter extends Rdb
{

    /** @var Credis */
    public $credis;


    /** @inheritdoc */
    protected function __construct($config)
    {
        parent::__construct($config);
        $config = $this->config;

        $standalone = $config->options['standalone'] ?? false;

        $this->credis = new Credis(
            $config->getHost(false),
            $config->getPort(),
            $config->timeout
        );

        if ($standalone) {
            $this->credis->forceStandalone();
        }

        $this->credis->connect();
        $this->credis->select($config->database);
    }


    /**
     * Credis can't guarantee that `setOption('prefix')` will always exist.
     *
     * So we're manually prefixing everything here. Yay!
     *
     * @param iterable $keys
     * @return string[]
     */
    protected function prefixKeys(iterable $keys): array
    {
        $keys = self::flattenArrays($keys);
        if (empty($keys)) return [];

        if ($this->config->prefix) {
            $keys = self::prefix($this->config->prefix, $keys);
            $keys = iterator_to_array($keys, false);
        }

        return $keys;
    }


    /** @inheritdoc */
    public function flushAll(bool $async = false)
    {
        if ($this->credis->isStandalone()) {
            $this->credis->__call('flushall', [$async ? 'ASYNC' : 'SYNC']);
        }
        else {
            $this->credis->__call('flushall', [$async]);
        }
    }


    /** @inheritdoc */
    public function flushDb(bool $async = false)
    {
        if ($this->credis->isStandalone()) {
            $this->credis->__call('flushdb', [$async ? 'ASYNC' : 'SYNC']);
        }
        else {
            $this->credis->__call('flushdb', [$async]);
        }
    }


    /** @inheritdoc */
    public function select(int $database): bool
    {
        return (bool) $this->credis->select($database);
    }


    /** @inheritdoc */
    public function move(string $key, int $database): bool
    {
        $key = $this->config->prefix . $key;
        return (bool) $this->credis->move($key, $database);
    }


    /** @inheritdoc */
    public function registerSessionHandler(string $prefix = 'session:'): bool
    {
        if (
            !$this->credis->isStandalone()
            and !empty($this->config->options['use_native_session'])
        ) {
            ini_set('session.save_handler', 'redis');
            ini_set('session.save_path', vsprintf('tcp://%s:%s?prefix=%s', [
                $this->config->getHost(false),
                $this->config->getPort(),
                $prefix,
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
            $keys = $this->credis->scan($it, $pattern, $this->config->scan_size);

            // If it's backed by php-redis it might return false.
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
        $key = $this->config->prefix . $key;
        return $this->credis->dump($key) ?: null;
    }


    /** @inheritdoc */
    public function restore(string $key, int $ttl, string $value, array $flags = []): bool
    {
        $key = $this->config->prefix . $key;
        $flags = $this->parseRestoreFlags($flags);

        if ($this->credis->isStandalone()) {
            $options = [];

            if ($flags['replace']) {
                $options[] = 'REPLACE';
            }

            $ok = $this->credis->restore($key, $ttl, $value, ...$options);
        }
        else {
            if ($flags['replace']) {
                $this->del($key);
            }

            $ok = $this->credis->restore($key, $ttl, $value);
        }

        return $ok !== false;
    }


    /** @inheritdoc */
    public function ttl(string $key): ?int
    {
        $key = $this->config->prefix . $key;
        $value = $this->credis->__call('pttl', [$key]);
        if (!is_numeric($value))  return null;
        return $value;
    }


    /** @inheritdoc */
    public function expire(string $key, $ttl = 0): bool
    {
        $key = $this->config->prefix . $key;
        return (bool) $this->credis->__call('pexpire', [$key, $ttl]);
    }


    /** @inheritdoc */
    public function expireAt(string $key, $ttl = 0): bool
    {
        $key = $this->config->prefix . $key;
        return (bool) $this->credis->__call('pexpireat', [$key, $ttl]);
    }


    /** @inheritdoc */
    public function rename(string $src, string $dst): bool
    {
        $src = $this->config->prefix . $src;
        $dst = $this->config->prefix . $dst;
        return (bool) $this->credis->rename($src, $dst);
    }


    /** @inheritdoc */
    public function type(string $key): ?string
    {
        $key = $this->config->prefix . $key;
        $type = $this->credis->type($key);
        if ($type === 'none') return null;
        return $type;
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

        $key = $this->config->prefix . $key;
        $result = $this->credis->set($key, $value, $options);

        // TODO Uhh.. does getset actually work here?
        if ($flags['get_set']) {
            return $result === false ? null : $result;
        }

        return (bool) $result;
    }


    /** @inheritdoc */
    public function append(string $key, string $value): int
    {
        $key = $this->config->prefix . $key;
        return $this->credis->append($key, $value);
    }


    /** @inheritdoc */
    public function get(string $key): ?string
    {
        $key = $this->config->prefix . $key;
        $result = $this->credis->get($key);
        if (!is_string($result)) return null;
        return $result;
    }


    /** @inheritdoc */
    public function getRange(string $key, int $from = 0, int $to = -1): ?string
    {
        $key = $this->config->prefix . $key;
        $result = $this->credis->getRange($key, $from, $to);
        if (!is_string($result)) return null;
        return $result;
    }


    /** @inheritdoc */
    public function mGet(iterable $keys): array
    {
        $prefixed = $this->prefixKeys($keys);
        if (empty($prefixed)) return [];

        $items = $this->credis->mGet($prefixed);

        foreach ($items as &$item) {
            if ($item !== false) continue;
            $item = null;
        }
        unset($item);

        $keys = self::normalizeIterable($keys);
        $items = array_combine($keys, $items);
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
    public function sAdd(string $key, ...$values): ?int
    {
        if (empty($values)) return 0;
        $key = $this->config->prefix . $key;
        return $this->credis->sAdd($key, ...$values);
    }


    /** @inheritdoc */
    public function sMembers(string $key): ?array
    {
        $key = $this->config->prefix . $key;

        if ($this->config->scan_keys) {
            $items = $this->sScan($key);
            return iterator_to_array($items, false);
        }

        return $this->credis->sMembers($key);
    }


    /** @inheritdoc */
    public function sScan(string $key, string $pattern = null): Generator
    {
        $pattern = $pattern ?: '*';
        $key = $this->config->prefix . $key;

        $it = null;

        for (;;) {
            $items = $this->credis->sscan($it, $key, $pattern, $this->config->scan_size);

            // If it's backed by php-redis it might return false.
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

        $key = $this->config->prefix . $key;
        return $this->credis->sRem($key, $values);
    }


    /** @inheritdoc */
    public function sCard(string $key): ?int
    {
        $key = $this->config->prefix . $key;
        return $this->credis->sCard($key);
    }


    /** @inheritdoc */
    public function sIsMember(string $key, string $value): ?bool
    {
        $key = $this->config->prefix . $key;
        $ok = $this->credis->sIsMember($key, $value);
        /** @var int|null $ok - another funky typing. */
        if ($ok === null) return null;
        return (bool) $ok;
    }


    /** @inheritdoc */
    public function sMove(string $src, string $dst, string $value): ?bool
    {
        $src = $this->config->prefix . $src;
        $dst = $this->config->prefix . $dst;

        $ok = $this->credis->sMove($src, $dst, $value);
        /** @var int|null $ok - another funky typing. */
        if ($ok === null) return null;
        return (bool) $ok;
    }


    /** @inheritdoc */
    public function incrBy(string $key, int $amount): int
    {
        $key = $this->config->prefix . $key;
        return $this->credis->incrBy($key, $amount);
    }


    /** @inheritdoc */
    public function incrByFloat(string $key, float $amount): float
    {
        $key = $this->config->prefix . $key;
        return (float) $this->credis->incrByFloat($key, $amount);
    }


    /** @inheritdoc */
    public function decrBy(string $key, int $amount): int
    {
        $key = $this->config->prefix . $key;
        return $this->credis->decrBy($key, $amount);
    }


    /** @inheritdoc */
    public function lPush(string $key, ...$items): ?int
    {
        $key = $this->config->prefix . $key;
        $items = $this->flatten($items, 2);
        $count = $this->credis->lPush($key, ...$items);
        if (!is_numeric($count)) return null;
        return $count;
    }


    /** @inheritdoc */
    public function rPush(string $key, ...$items): ?int
    {
        $key = $this->config->prefix . $key;
        $items = $this->flatten($items, 2);
        $count = $this->credis->rPush($key, ...$items);
        if (!is_numeric($count)) return null;
        return $count;
    }


    /** @inheritdoc */
    public function lPop(string $key): ?string
    {
        $key = $this->config->prefix . $key;
        return $this->credis->lPop($key);
    }


    /** @inheritdoc */
    public function rPop(string $key): ?string
    {
        $key = $this->config->prefix . $key;
        return $this->credis->rPop($key);
    }


    /** @inheritdoc */
    public function rPoplPush(string $src, string $dst): ?string
    {
        $src = $this->config->prefix . $src;
        $dst = $this->config->prefix . $dst;

        return $this->credis->rPoplPush($src, $dst);
    }


    /** @inheritdoc */
    public function lRange(string $key, int $start = 0, int $stop = -1): ?array
    {
        $key = $this->config->prefix . $key;
        return $this->credis->lRange($key, $start, $stop);
    }


    /** @inheritdoc */
    public function lTrim(string $key, int $start = 0, int $stop = -1): ?bool
    {
        $key = $this->config->prefix . $key;
        return $this->credis->lTrim($key, $start, $stop);
    }


    /** @inheritdoc */
    public function lLen(string $key): ?int
    {
        $key = $this->config->prefix . $key;
        $count = $this->credis->lLen($key);
        if (!is_numeric($count)) return null;
        return $count;
    }


    /** @inheritdoc */
    public function lSet(string $key, int $index, string $item): ?bool
    {
        $key = $this->config->prefix . $key;
        return $this->credis->lSet($key, $index, $item);
    }


    /** @inheritdoc */
    public function lIndex(string $key, int $index): ?string
    {
        $key = $this->config->prefix . $key;
        return $this->credis->lIndex($key, $index);
    }


    /** @inheritdoc */
    public function lRem(string $key, string $item, int $count = 0): ?int
    {
        // Args item/count are swapped.
        $key = $this->config->prefix . $key;
        return $this->credis->lRem($key, $count, $item);
    }


    /** @inheritdoc */
    public function blPop($keys, int $timeout = null): ?array
    {
        if (is_scalar($keys)) {
            $keys = $this->config->prefix . $keys;
            $keys = [$keys];
        }
        else {
            $keys = $this->prefixKeys($keys);
            // TODO it's not a timeout though.
            // But an empty array is less clear - null is only better because
            // it might prevent an infinite loop.
            if (empty($keys)) return null;
        }

        if ($timeout === null) {
            $timeout = $this->config->timeout;
        }

        // I can only assume the typings are lying.
        $args = $keys;
        $args[] = $timeout;

        $value = $this->credis->__call('blPop', $args);
        if (empty($value)) return null;
        return $value;
    }


    /** @inheritdoc */
    public function brPop($keys, int $timeout = null): ?array
    {
        if (is_scalar($keys)) {
            $keys = $this->config->prefix . $keys;
            $keys = [$keys];
        }
        else {
            $keys = self::prefixKeys($keys);
        }

        if ($timeout === null) {
            $timeout = $this->config->timeout;
        }

        // I can only assume the typings are lying.
        $args = $keys;
        $args[] = $timeout;

        $value = $this->credis->__call('brPop', $args);
        if (empty($value)) return null;
        return $value;
    }


    /** @inheritdoc */
    public function brPoplPush(string $src, string $dst, int $timeout = null): ?string
    {
        $src = $this->config->prefix . $src;
        $dst = $this->config->prefix . $dst;

        if ($timeout === null) {
            $timeout = $this->config->timeout;
        }

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


    /** @inheritdoc */
    public function zAdd(string $key, array $members): int
    {
        $key = $this->config->prefix . $key;

        $args = [];
        $args[] = $key;

        foreach ($members as $member => $score) {
            $args[] = $score;
            $args[] = $member;
        }

        $res = $this->credis->__call('zadd', $args);
        return (int) $res;
    }


    /** @inheritdoc */
    public function zIncrBy(string $key, float $value, string $member): float
    {
        $key = $this->config->prefix . $key;
        return (float) $this->credis->zIncrBy($key, $value, $member);
    }


    /** @inheritdoc */
    public function zRange(string $key, $start = null, $stop = null, array $flags = []): ?array
    {
        $key = $this->config->prefix . $key;

        $flags = self::parseRangeFlags($flags);

        $cmd = $flags['rev'] ? 'zRevRange' : 'zRange';
        $args = [];
        $args[] = $key;

        $options = [];

        if ($flags['bylex']) {
            $cmd .= 'ByLex';

            if ($start and !preg_match('/^\[|^\(/', $start)) {
                $start = '[' . $start;
            }
            if ($stop and !preg_match('/^\[|^\(/', $stop)) {
                $stop = '[' . $stop;
            }

            $start = $start ?? '-';
            $stop = $stop ?? '+';

            $args[] = $start;
            $args[] = $stop;

            if ($flags['limit']) {
                $args[] = 'LIMIT';
                $args[] = $flags['limit']['offset'];
                $args[] = $flags['limit']['count'];
            }
        }
        else if ($flags['byscore']) {
            $cmd .= 'ByScore';

            $args[] = $start ?? '-inf';
            $args[] = $stop ?? '+inf';

            if ($flags['withscores']) {
                $options['withscores'] = true;
            }

            if ($flags['limit']) {
                $options['limit'] = [
                    $flags['limit']['offset'],
                    $flags['limit']['count'],
                ];
            }
        }
        else {
            $args[] = $start ?? 0;
            $args[] = $stop ?? -1;

            if ($flags['withscores']) {
                $options['withscores'] = true;
            }
        }

        if ($options) {
            $args[] = $options;
        }

        /** @var array|false $range */
        $range = $this->credis->__call($cmd, $args);
        if ($range === false) return null;
        return $range;
    }


    /** @inheritdoc */
    public function zRem(string $key, ...$members): int
    {
        $key = $this->config->prefix . $key;

        $args = self::flattenArrays($members);
        array_unshift($args, $key);

        $value = $this->credis->__call('zrem', $args);
        return (int) $value;
    }


    /** @inheritdoc */
    public function zCard(string $key): ?int
    {
        $key = $this->config->prefix . $key;
        return $this->credis->zCard($key);
    }


    /** @inheritdoc */
    public function zCount(string $key, float $min, float $max): ?int
    {
        $key = $this->config->prefix . $key;
        return $this->credis->zCount($key, $min, $max);
    }


    /** @inheritdoc */
    public function zScore(string $key, string $member): ?float
    {
        $key = $this->config->prefix . $key;
        $score = $this->credis->__call('zscore', [$key, $member]);
        if (!is_numeric($score)) return null;
        return (float) $score;
    }


    /** @inheritdoc */
    public function zRank(string $key, string $member): ?int
    {
        $key = $this->config->prefix . $key;
        return $this->credis->zRank($key, $member);
    }


    /** @inheritdoc */
    public function zRevRank(string $key, string $member): ?int
    {
        $key = $this->config->prefix . $key;
        return $this->credis->zRevRank($key, $member);
    }


    /** @inheritdoc */
    public function hDel(string $key, ...$fields): int
    {
        $key = $this->config->prefix . $key;
        $fields = self::flatten($fields);
        return (int) $this->credis->hDel($key, ...$fields);
    }


    /** @inheritdoc */
    public function hExists(string $key, string $field): bool
    {
        $key = $this->config->prefix . $key;
        return (bool) $this->credis->hExists($key, $field);
    }


    /** @inheritdoc */
    public function hSet(string $key, string $field, $value, bool $replace = true): bool
    {
        $key = $this->config->prefix . $key;
        if ($replace) {
            $ok = $this->credis->hSet($key, $field, $value);
        }
        else {
            $ok = $this->credis->hSetNx($key, $field, $value);
        }
        return (bool) $ok;
    }


    /** @inheritdoc */
    public function hGet(string $key, string $field): ?string
    {
        $key = $this->config->prefix . $key;
        $value = $this->credis->hGet($key, $field);
        if ($value === false) return null;
        return $value;
    }


    /** @inheritdoc */
    public function hGetAll(string $key): ?array
    {
        $key = $this->config->prefix . $key;
        $value = $this->credis->hGetAll($key);
        if (!is_array($value) or empty($value)) return null;
        return $value;
    }


    /** @inheritdoc */
    public function hIncrBy(string $key, string $field, int $amount): int
    {
        $key = $this->config->prefix . $key;
        return $this->credis->hIncrBy($key, $field, $amount);
    }


    /** @inheritdoc */
    public function hIncrByFloat(string $key, string $field, float $amount): float
    {
        $key = $this->config->prefix . $key;
        return (float) $this->credis->hIncrByFloat($key, $field, $amount);
    }


    /** @inheritdoc */
    public function hStrLen(string $key, string $field): ?int
    {
        $key = $this->config->prefix . $key;
        $res = $this->credis->hStrLen($key, $field);
        if (!is_numeric($res)) return null;
        return (int) $res;
    }


    /** @inheritdoc */
    public function hKeys(string $key): ?array
    {
        $key = $this->config->prefix . $key;
        $res = $this->credis->hKeys($key);
        if (!is_array($res) or empty($res)) return null;
        return $res;
    }


    /** @inheritdoc */
    public function hVals(string $key): ?array
    {
        $key = $this->config->prefix . $key;
        $res = $this->credis->hVals($key);
        if (!is_array($res) or empty($res)) return null;
        return $res;
    }


    /** @inheritdoc */
    public function hLen(string $key): int
    {
        $key = $this->config->prefix . $key;
        $res = $this->credis->hLen($key);
        if (!is_numeric($res)) return 0;
        return (int) $res;
    }


    /** @inheritdoc */
    public function hmGet(string $key, ...$fields): ?array
    {
        $key = $this->config->prefix . $key;
        $fields = self::flatten($fields);
        $values = $this->credis->hMGet($key, $fields);
        if (!is_array($values)) return null;
        return array_values($values);
    }


    /** @inheritdoc */
    public function hmSet(string $key, array $fields): bool
    {
        $key = $this->config->prefix . $key;
        $ok = $this->credis->hMSet($key, $fields);
        return (bool) $ok;
    }


    /** @inheritdoc */
    public function hScan(string $key, string $pattern = null): Generator
    {
        $key = $this->config->prefix . $key;
        $pattern = $pattern ?: '*';
        $it = null;

        for (;;) {
            $items = $this->credis->hScan($it, $key, $pattern, $this->config->scan_size);
            if ($items === false) break;

            foreach ($items as $key => $item) {
                yield $key => $item;
            }

            // The iterator is done.
            // Keys might not be empty though, so do this last.
            if (!$it) break;
        }
    }
}
