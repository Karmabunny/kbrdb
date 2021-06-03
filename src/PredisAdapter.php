<?php
/**
 * @link      https://github.com/Karmabunny
 * @copyright Copyright (c) 2021 Karmabunny
 */

namespace karmabunny\rdb;

use Generator;
use Predis\Client;


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

        $this->predis = new Client($config->getHost(true), $options);
        $this->predis->connect();
    }


    /** @inheritdoc */
    public function keys(string $pattern): array
    {
        $keys = $this->predis->keys($pattern);
        $keys = $this->stripPrefix(...$keys);
        return $keys;
    }


    /** @inheritdoc */
    public function scan(string $pattern): Generator
    {
        $offset = 0;

        while (true) {
            $keys = $this->predis->scan($offset, [
                'MATCH' => $pattern,
                'COUNT' => $this->config->chunk_size,
            ]);

            if (empty($keys)) break;

            $offset += count($keys);

            foreach ($keys as $key) {
                yield $this->stripPrefix($key);
            }
        }
    }


    /** @inheritdoc */
    public function set(string $key, $value, $ttl = 0): bool
    {
        $args[] = $key;
        $args[] = $value;

        if ($ttl) {
            $args[] = 'PX';
            $args[] = $ttl;
        }

        // Fun hack because predis is built with weird command interfaces.
        // Legit, this doesn't work: set($key, $value, null, null);
        return (bool) @call_user_func_array([$this->predis, 'set'], $args);
    }


    /** @inheritdoc */
    public function get(string $key): ?string
    {
        return $this->predis->get($key);
    }


    /** @inheritdoc */
    public function mGet(array $keys): array
    {
        if (empty($keys)) return [];
        return $this->predis->mget($keys);
    }


    /** @inheritdoc */
    public function mSet(array $items): bool
    {
        if (empty($values)) return false;
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
