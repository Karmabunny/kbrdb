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
 * - chunk_size
 * - options (adapter specific)
 *
 * Also:
 * - lock_sleep
 * - scan_keys
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

    /** @var string RdbConfig::TYPE */
    public $adapter = self::TYPE_PREDIS;

    /** @var int */
    public $chunk_size = 50;

    /** @var int in seconds - connection timeout */
    public $timeout = 5;

    /** @var int in milliseconds */
    public $lock_sleep = 5;

    /** @var array */
    public $options = [];

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
        // No scheme/port, just a plain old hostname.
        if (strpos($this->host, ':') === false) {
            return $this->host;
        }

        $url = '';

        if ($scheme = parse_url($this->host, PHP_URL_SCHEME)) {
            $url .= $scheme . '://';
        }

        $url .= parse_url($this->host, PHP_URL_HOST);

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
        if (strpos($this->host, ':') !== false) {
            $port = parse_url($this->host, PHP_URL_PORT);
            if ($port) return $port;
        }

        return 6379;
    }
}
