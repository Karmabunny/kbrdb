<?php
/**
 * @link      https://github.com/Karmabunny
 * @copyright Copyright (c) 2021 Karmabunny
 */

namespace karmabunny\rdb;

use Credis_Client;
use Generator;


/**
 * Rdb using the predis library.
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


    /** @inheritdoc */
    public function keys(string $pattern): array
    {
        $keys = $this->credis->keys($pattern);
        $keys = $this->stripPrefix(...$keys);
        return $keys;
    }


    /** @inheritdoc */
    public function scan(string $pattern): Generator
    {
        $pattern = $this->config->prefix . $pattern;
        $it = null;

        while ($keys = $this->credis->scan($it, $pattern, $this->config->chunk_size)) {
            foreach ($keys as $key) {
                yield $this->stripPrefix($key);
            }
        }
    }


    /** @inheritdoc */
    public function set(string $key, $value, $ttl = 0): bool
    {
        $options = [];

        if ($ttl) {
            $options['px'] = $ttl;
        }

        $key = $this->config->prefix . $key;
        return $this->redis->set($key, $value, $options);
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
    public function mGet(array $keys): array
    {
        if (empty($keys)) return [];

        foreach ($keys as &$key) {
            $key = $this->config->prefix . $key;
        }
        unset($key);

        return $this->credis->mget($keys);
    }


    /** @inheritdoc */
    public function mSet(array $items): bool
    {
        if (empty($values)) return false;

        $keys = [];
        foreach ($items as $key => $item) {
            $keys[] = $this->config->prefix . $key;
        }

        $items = array_combine($keys, $items);

        return (bool) @$this->credis->mset($items);
    }


    /** @inheritdoc */
    public function sAdd(string $key, ...$values): int
    {
        if (empty($values)) return 0;
        $key = $this->config->prefix . $key;
        return $this->credis->sadd($key, ...$values);
    }


    /** @inheritdoc */
    public function sMembers(string $key): array
    {
        $key = $this->config->prefix . $key;
        return $this->credis->smembers($key);
    }


    /** @inheritdoc */
    public function sRem(string $key, ...$values): int
    {
        if (empty($values)) return 0;
        $key = $this->config->prefix . $key;
        return $this->credis->srem($key, $values);
    }


    /** @inheritdoc */
    public function exists(...$keys): int
    {
        if (empty($keys)) return 0;

        foreach ($keys as &$key) {
            $key = $this->config->prefix . $key;
        }
        unset($key);

        return $this->credis->exists(...$keys);
    }


    /** @inheritdoc */
    public function del(...$keys): int
    {
        $keys = self::expandArrays($keys);

        foreach ($keys as &$key) {
            $key = $this->config->prefix . $key;
        }
        unset($key);

        return $this->credis->del($keys);
    }

}
