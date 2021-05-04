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
        $keys = $this->redis->keys($pattern);
        $keys = $this->stripPrefix(...$keys);
        return $keys;
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
    public function set(string $key, string $value, $ttl = 0): bool
    {
        $options = [];

        if ($ttl) {
            $options['px'] = $ttl;
        }

        return $this->redis->set($key, $value, $options);
    }


    /** @inheritdoc */
    public function get(string $key): ?string
    {
        return $this->redis->get($key);
    }


    /** @inheritdoc */
    public function mGet(array $keys): array
    {
        return $this->redis->mGet($keys);
    }


    /** @inheritdoc */
    public function mSet(array $items): bool
    {
        return $this->redis->mSet($items);
    }


    /** @inheritdoc */
    public function sAdd(string $key, ...$values): int
    {
        $values = self::expandArrays($values);
        return $this->redis->sAdd($key, ...$values);
    }


    /** @inheritdoc */
    public function sMembers(string $key): array
    {
        return $this->redis->smembers($key);
    }

    /** @inheritdoc */
    public function exists(...$keys): int
    {
        $keys = self::expandArrays($keys);
        return $this->redis->exists($keys);
    }


    /** @inheritdoc */
    public function del(...$keys): int
    {
        $keys = self::expandArrays($keys);
        return $this->redis->del($keys);
    }

}
