<?php
/**
 * @link      https://github.com/Karmabunny
 * @copyright Copyright (c) 2021 Karmabunny
 */

namespace karmabunny\rdb;


/**
 * Configuration for Rdb.
 *
 * Common properties:
 * - host
 * - prefix
 * - adapter
 *
 * Also:
 * - chunk_size
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

    /** @var string */
    public $host = '127.0.0.1';

    /** @var string */
    public $prefix = '';

    /** @var int */
    public $database = 0;

    /** @var string RdbConfig::TYPE */
    public $adapter = self::TYPE_PREDIS;

    /** @var int for mscan and friends (mScanObjects) */
    public $chunk_size = 50;

    /** @var int for scan and friends (sscan, hscan, zscan) */
    public $scan_size = 1000;

    /** @var int in seconds - connection timeout */
    public $timeout = 5;

    /** @var int in milliseconds */
    public $lock_sleep = 5;

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
    public $scan_keys = false;

    /** @var array */
    public $options = [];

    /**
     * Create a new config object.
     *
     * @param iterable $config
     */
    public function __construct($config)
    {
        foreach ($config as $key => $value) {
            $this->$key = $value;
        }
    }


    /**
     * Get the hostname.
     *
     * @param bool $port Include the port number.
     * @return string
     */
    public function getHost($port = false): string
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
