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
        $pattern = $this->config->prefix . $pattern;
        $keys = $this->credis->keys($pattern);
        return array_map([$this, 'stripPrefix'], $keys);
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
        if (empty($values)) return false;

        $keys = [];
        foreach ($items as $key => $item) {
            $keys[] = $this->config->prefix . $key;
        }

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
        return $this->credis->incrby($key, $amount);
    }


    /** @inheritdoc */
    public function decr(string $key, int $amount = 1): int
    {
        $key = $this->config->prefix . $key;
        return $this->credis->decrby($key, $amount);
    }


    /** @inheritdoc */
    public function exists(...$keys): int
    {
        if (empty($keys)) return 0;
        $keys = $this->prefixKeys($keys);
        if (empty($keys)) return 0;

        return $this->credis->exists(...$keys);
    }


    /** @inheritdoc */
    public function del(...$keys): int
    {
        if (empty($keys)) return 0;
        $keys = $this->prefixKeys($keys);
        if (empty($keys)) return 0;

        return $this->credis->del(...$keys);
    }

}
