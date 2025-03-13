<?php
/**
 * @link      https://github.com/Karmabunny
 * @copyright Copyright (c) 2021 Karmabunny
 */

namespace karmabunny\rdb;

use JsonException;

/**
 * Leaky bucket rate limiting.
 *
 * @package karmabunny\rdb
 */
class RdbBucket
{

    /** @var string */
    public $key;

    /** @var string */
    public $prefix = 'drip:';

    /** @var int Bucket size. */
    public $capacity = 60;

    /** @var float Drips per second. */
    public $drip_rate = 1;

    /** @var int[] [ name => drip size ] */
    public $costs = [];


    /** @var Rdb */
    protected $rdb;

    /** @var array */
    protected $drips = [];


    /**
     * Create a new bucket.
     *
     * The config is this:
     * - key
     * - prefix (def: 'drip:')
     * - capacity (def: 60)
     * - drip_rate (def: 1)
     * - costs
     *
     * @param Rdb $rdb
     * @param string|array $config
     */
    public function __construct(Rdb $rdb, $config)
    {
        // Shorthand config.
        if (is_string($config)) {
            $config = [ 'key' => $config ];
        }

        foreach ($config as $key => $value) {
            if (!property_exists($this, $key)) continue;
            $this->$key = $value;
        }

        $this->rdb = $rdb;

        // Fetch and clean the drips.
        $this->refresh();
    }


    /**
     *
     * @return void
     * @throws JsonException
     */
    public function refresh()
    {
        $this->drips = $this->purge($this->load());
    }


    /**
     * Get the drips.
     *
     * @return array
     */
    protected function load()
    {
        $drips = $this->rdb->getJson($this->prefix . $this->key);
        if (!is_array($drips)) $drips = [];
        return $drips;
    }


    /**
     * Save the drips.
     *
     * @return void
     */
    protected function save()
    {
        $this->rdb->setJson($this->prefix . $this->key, $this->drips);
    }


    /**
     * Clean out old drips.
     *
     * @param array $drips
     * @return array
     */
    protected function purge(array $drips): array
    {
        $period = $this->capacity / $this->drip_rate;
        $expiry = microtime(true) - $period;

        foreach ($drips as $key => $drip) {
            if ($drip >= $expiry) continue;
            unset($drips[$key]);
        }

        return $drips;
    }


    /**
     * Time to wait (ms) before the next drip.
     *
     * This will be zero if the bucket isn't full.
     *
     * @return int milliseconds
     */
    public function getWait(): int
    {
        if (!$this->isFull()) return 0;

        // The time between now and the oldest drip.
        $time = microtime(true) - min($this->drips);

        $period = $this->capacity / $this->drip_rate;
        $period -= $time;
        return (int) ($period * 1000);
    }


    /**
     * What's the water level of the bucket?
     *
     * @return int number of drips
     */
    public function getLevel(): int
    {
        return count($this->drips);
    }


    /**
     * Is the bucket full?
     *
     * @return bool
     */
    public function isFull(): bool
    {
        return $this->getLevel() >= $this->capacity;
    }


    /**
     * Record a drip in to the bucket.
     *
     * Either provide a number to specify a number of drips or a string
     * to use a configured 'cost'.
     *
     * For example, per request:
     * - 'GET' might be 1 drip
     * - 'POST' is 5 drips
     *
     * Or, per region:
     * - 'AU' is 1 drip
     * - 'US' is 2 drips
     *
     * @param int|string $size drop size or cost lookup
     *  - `int` - How many drips
     *  - `string` - A cost name
     * @return bool
     *  - `false` - the bucket is full, did not add the drip
     *  - `true` - the drip was added
     */
    public function drip($size = 1): bool
    {
        // Busted!
        if ($this->isFull()) return false;

        // Cost lookup.
        if (is_string($size)) {
            $size = $this->costs[$size] ?? 1;
        }

        // Doesn't fit.
        if ($size + $this->getLevel() > $this->capacity) {
            return false;
        }

        $timestamp = microtime(true);

        for ($i = 0; $i < $size; $i++) {
            $this->drips[] = $timestamp;
        }

        $this->save();
        return true;
    }


    /**
     * Get bucket info.
     *
     * These are typically used as headers to help a client figure how to not
     * bust the rate limit.
     *
     * @return array
     */
    public function getStatus()
    {
        $level = $this->getLevel();
        $wait = $this->getWait();

        return [
            'level' => "{$level}/{$this->capacity}",
            'drip_rate' => sprintf('%.2f', $this->drip_rate),
            'wait' => $wait,
        ];
    }


    /**
     * Write out bucket info to the headers.
     *
     * @return void
     */
    public function writeHeaders()
    {
        $headers = $this->getStatus();
        foreach ($headers as $name => $value) {
            $name = str_replace('_', '-', $name);
            header("x-bucket-{$name}: {$value}");
        }
    }

}
