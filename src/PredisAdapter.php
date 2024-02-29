<?php
/**
 * @link      https://github.com/Karmabunny
 * @copyright Copyright (c) 2021 Karmabunny
 */

namespace karmabunny\rdb;

use Generator;
use karmabunny\rdb\Wrappers\Predis;
use Predis\Client;
use Predis\Collection\Iterator\Keyspace;
use Predis\Connection\Parameters;
use Predis\Response\Status;
use Predis\Session\Handler;

/**
 * Rdb using the predis library.
 *
 * Adapter options are documented in predis. Rdb will naturally clobber
 * the `prefix` and `timeout` parameters.
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

        $options = $this->config->options;
        $options['prefix'] = $this->config->prefix;
        $options['timeout'] = $this->config->timeout;

        // Just to be clear about what parameters we're using.
        $host = $this->config->getHost(false);

        $parameters = Parameters::parse($host);
        $parameters['port'] = $this->config->getPort();
        $parameters = new Parameters($parameters);

        $this->predis = new Predis($parameters, $options);
        $this->predis->connect();
    }


    /** @inheritdoc */
    public function registerSessionHandler(string $prefix = 'session:'): bool
    {
        // We're creating a new client here, but with a modified prefix.
        $options = $this->config->options;
        $options['prefix'] = $this->config->prefix . $prefix;
        $options['timeout'] = $this->config->timeout;

        $predis = new Predis($this->config->getHost(true), $options);
        $handler = new Handler($predis);

        return session_set_save_handler($handler, true);
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
    public function dump(string $key): ?string
    {
        return $this->predis->dump($key);
    }


    /** @inheritdoc */
    public function restore(string $key, int $ttl, string $value, array $flags = []): bool
    {
        $flags = $this->parseRestoreFlags($flags);

        $args = [$key, $ttl, $value];

        if ($flags['replace']) {
            $args[] = 'REPLACE';
        }

        /** @var Status $status */
        $status = @call_user_func_array([$this->predis, 'restore'], $args);
        return $status == 'OK';
    }


    /** @inheritdoc */
    public function ttl(string $key): ?int
    {
        return $this->predis->pttl($key);
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
        $type = (string) $this->predis->type($key);
        if ($type === 'none') return null;
        return $type;
    }


    /** @inheritdoc */
    public function set(string $key, string $value, int $ttl = 0, array $flags = [])
    {
        $flags = self::parseSetFlags($flags);

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
    public function append(string $key, string $value): int
    {
        return (int) $this->predis->append($key, $value);
    }


    /** @inheritdoc */
    public function get(string $key): ?string
    {
        return $this->predis->get($key);
    }


    /** @inheritdoc */
    public function getRange(string $key, int $from = 0, int $to = -1): ?string
    {
        return $this->predis->getrange($key, $from, $to);
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
    public function sAdd(string $key, ...$values): ?int
    {
        $values = self::flattenArrays($values);
        if (empty($values)) return 0;

        return $this->predis->sadd($key, $values);
    }


    /** @inheritdoc */
    public function sMembers(string $key): ?array
    {
        return $this->predis->smembers($key);
    }


    /** @inheritdoc */
    public function sRem(string $key, ...$values): ?int
    {
        $values = self::flattenArrays($values);
        if (empty($values)) return 0;

        return $this->predis->srem($key, $values);
    }


    /** @inheritdoc */
    public function sIsMember(string $key, string $value): ?bool
    {
        $ok = $this->predis->sismember($key, $value);
        /** @var int|null $ok */
        if ($ok === null) return null;
        return (bool) $ok;
    }


    /** @inheritdoc */
    public function sCard(string $key): ?int
    {
        return $this->predis->scard($key);
    }


    /** @inheritdoc */
    public function sMove(string $src, string $dst, string $value): ?bool
    {
        $ok = $this->predis->smove($src, $dst, $value);
        /** @var int|null $ok - kinda weird typing here. */
        if ($ok === null) return null;
        return (bool) $ok;
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
        return $count;
    }


    /** @inheritdoc */
    public function rPush(string $key, ...$items): ?int
    {
        $count = $this->predis->rpush($key, $items);
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
    public function lRange(string $key, int $start = 0, int $stop = -1): ?array
    {
        $range = $this->predis->lrange($key, $start, $stop);
        return $range;
    }


    /** @inheritdoc */
    public function lTrim(string $key, int $start = 0, int $stop = -1): ?bool
    {
        return $this->predis->ltrim($key, $start, $stop);
    }


    /** @inheritdoc */
    public function lLen(string $key): ?int
    {
        return $this->predis->llen($key);
    }


    /** @inheritdoc */
    public function lSet(string $key, int $index, string $item): ?bool
    {
        return $this->predis->lset($key, $index, $item);
    }


    /** @inheritdoc */
    public function lIndex(string $key, int $index): ?string
    {
        return $this->predis->lindex($key, $index);
    }


    /** @inheritdoc */
    public function lRem(string $key, string $item, int $count = 0): ?int
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


    /** @inheritdoc */
    public function zAdd(string $key, array $members): int
    {
        return $this->predis->zadd($key, $members);
    }


    /** @inheritdoc */
    public function zIncrBy(string $key, float $value, string $member): float
    {
        // @phpstan-ignore-next-line: yeah no, increment can be a float.
        return (float) $this->predis->zincrby($key, $value, $member);
    }


    /** @inheritdoc */
    public function zRange(string $key, $start = null, $stop = null, array $flags = []): ?array
    {
        $flags = self::parseRangeFlags($flags);

        $cmd = $flags['rev'] ? 'zRevRange' : 'zRange';

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
        }
        else if ($flags['byscore']) {
            $cmd .= 'ByScore';

            $start = $start ?? '-inf';
            $stop = $stop ?? '+inf';
        }
        else {
            $start = $start ?? 0;
            $stop = $stop ?? -1;
        }

        $args = [
            'WITHSCORES' => $flags['withscores'],
            'LIMIT' => $flags['limit'],
        ];

        $range = $this->predis->$cmd($key, $start, $stop, $args);
        return $range;
    }


    /** @inheritdoc */
    public function zRem(string $key, ...$members): int
    {
        $args = self::flattenArrays($members);
        array_unshift($args, $key);

        return (int) @call_user_func_array([$this->predis, 'zrem'], $args);
    }


    /** @inheritdoc */
    public function zCard(string $key): ?int
    {
        return $this->predis->zcard($key);
    }


    /** @inheritdoc */
    public function zCount(string $key, float $min, float $max): ?int
    {
        // @phpstan-ignore-next-line: unsure actually but it worked previously.
        $count = $this->predis->zcount($key, $min, $max);
        if (!is_numeric($count)) return null;
        return (int) $count;
    }


    /** @inheritdoc */
    public function zScore(string $key, string $member): ?float
    {
        $score = $this->predis->zscore($key, $member);
        if ($score === null) return null;
        return (float) $score;
    }


    /** @inheritdoc */
    public function zRank(string $key, string $member): ?int
    {
        return $this->predis->zrank($key, $member);
    }


    /** @inheritdoc */
    public function zRevRank(string $key, string $member): ?int
    {
        return $this->predis->zrevrank($key, $member);
    }
}
