<?php
/**
 * @link      https://github.com/Karmabunny
 * @copyright Copyright (c) 2021 Karmabunny
 */

namespace karmabunny\rdb;

use karmabunny\rdb\Objects\PhpObjectDriver;

/**
 * Configuration for Rdb.
 *
 * Common properties:
 * - host
 * - prefix
 * - database
 * - adapter
 *
 * Also:
 * - object_driver
 * - chunk_size
 * - scan_size
 * - timeout
 * - lock_sleep
 * - scan_keys
 * - options (adapter specific)
 *
 * @package karmabunny\rdb
 */
class RdbConfig
{

    const TYPE_PREDIS = 'predis';

    const TYPE_PHP_REDIS = 'php-redis';

    const TYPE_CREDIS = 'credis';

    /**
     * Switch between TTL/PTTL commands if given a int or float, respectively.
     *
     * Given a float Rdb will multiply by 1000 for the PTTL command.
     */
    const TTL_AUTO = 'auto';

    /**
     * Always use whole seconds, the TTL command.
     *
     * The value is provided in seconds.
     */
    const TTL_INTEGER = 'integer';

    /**
     * Always use float seconds, the PTTL command.
     *
     * The value is provided in seconds.
     */
    const TTL_FLOAT = 'float';

    /**
     * Always use PTTL _and_ assume the value is provided as milliseconds.
     *
     * Use this for compatibility with v1/v2 series.
     *
     * @deprecated
     */
    const TTL_COMPAT = 'milliseconds';

    /** @var string */
    public string $host = '127.0.0.1';

    /** @var string */
    public string $prefix = '';

    /** @var int */
    public int $database = 0;

    /** @var string RdbConfig::TYPE */
    public string $adapter = self::TYPE_PREDIS;

    /** @var class-string<RdbObjectDriver> */
    public string $object_driver = PhpObjectDriver::class;

    /** @var int for mscan and friends (mScanObjects) */
    public int $chunk_size = 50;

    /** @var int for scan and friends (sscan, hscan, zscan) */
    public int $scan_size = 1000;

    /** @var int in seconds - connection timeout */
    public int $timeout = 5;

    /** @var float in seconds */
    public float $lock_sleep = 0.005;

    /**
     * Replace keys() with a scan().
     *
     * Warning! It's considerably slower but is a better citizen than `keys`.
     *
     * Because redis is single-threaded the iterative `scan` command helps
     * prevent other clients/connections/requests from blocking up the server.
     *
     * Using this setting may improve overall responsiveness when the database
     * is under increased load at the cost of immediate performance.
     *
     * @var bool
     */
    public bool $scan_keys = false;

    /**
     * - `TTL_AUTO`    use TTL/PTTL based on the value type (int or float, respectively)
     * - `TTL_INTEGER` use TTL only (int)
     * - `TTL_FLOAT`   use PTTL only (float)
     * - `TTL_COMPAT`  use PTTL (float) and assume the value is provided as milliseconds
     *
     * @var string RdbConfig::TTL
     */
    public string $ttl_mode = self::TTL_AUTO;

    /** @var array */
    public array $options = [];

    /**
     * Create a new config object.
     *
     * @param iterable $config
     */
    public function __construct(iterable $config)
    {
        foreach ($config as $key => $value) {
            if (!property_exists($this, $key)) continue;
            $this->$key = $value;
        }
    }


    /**
     * Get the hostname.
     *
     * @param bool $port Include the port number.
     * @return string
     */
    public function getHost(bool $port = false): string
    {
        $host = $this->host;

        // Throw in a default scheme.
        if (strpos($host, '://') === false) {
            $host = 'tcp://' . $host;
        }

        $url = '';

        if ($scheme = parse_url($host, PHP_URL_SCHEME)) {
            $url .= $scheme . '://';
        }

        $url .= parse_url($host, PHP_URL_HOST);

        if ($port) {
            $url .= ':' . $this->getPort();
        }

        return $url;
    }


    /**
     * Get the port number from the host.
     *
     * Returns the default (6379) otherwise.
     *
     * @return int
     */
    public function getPort(): int
    {
        $host = $this->host;

        // Throw in a default scheme.
        if (strpos($host, '://') === false) {
            $host = 'tcp://' . $host;
        }

        $port = parse_url($host, PHP_URL_PORT);
        if ($port) return $port;

        return 6379;
    }
}
