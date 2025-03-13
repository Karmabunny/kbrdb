<?php
/**
 * @link      https://github.com/Karmabunny
 * @copyright Copyright (c) 2021 Karmabunny
 */

namespace karmabunny\rdb;

use Exception;

/**
 * Dump stuff.
 *
 * @package karmabunny\rdb
 */
trait RdbDumpTrait
{


    /** @var Rdb */
    public $rdb;

    /** @var string */
    public $pattern;

    /** @var string[] */
    public $excludes = [];

    /** @var bool */
    public $compressed = true;

    /** @var callable|null */
    public $log = null;

    /** @var resource|null */
    protected $handle = null;


    /**
     * Create a dumper for this rdb + pattern.
     *
     * @param Rdb $rdb
     * @param string|array $config
     * - pattern (include pattern, default: '*')
     * - excludes (array of patterns)
     * - compressed (default: true)
     * - log (callback)
     *
     * Include + exclude patterns still respect the rdb's 'prefix'.
     */
    public function __construct(Rdb $rdb, $config = [])
    {
        $this->rdb = $rdb;

        // Backwards compat.
        if (is_string($config)) {
            $config = ['pattern' => $config];
        }

        if (empty($config['pattern'])) {
            $config['pattern'] = '*';
        }

        static $CONFIG = [ 'pattern', 'excludes', 'compressed', 'log' ];
        $config = array_intersect_key($config, array_fill_keys($CONFIG, true));

        foreach ($config as $key => $value) {
            if (!property_exists($this, $key)) continue;
            $this->$key = $value;
        }
    }


    /**
     * Set a logger callback.
     *
     * @param callable $log
     * @return void
     */
    public function setLog(callable $log)
    {
        $this->log = $log;
    }


    /**
     * Set an include pattern.
     *
     * Default is all keys - `*`.
     *
     * Note, that this is still subject to the rdb's 'prefix'.
     *
     * @param string $pattern
     * @return void
     */
    public function setPattern(string $pattern)
    {
        $this->pattern = $pattern;
    }


    /**
     *
     * @param string[]|string $pattern
     * @return void
     */
    public function setExclude($pattern)
    {
        $pattern = is_array($pattern) ? $pattern : [$pattern];
        $this->excludes = $pattern;
    }


    /**
     *
     * @param string $key
     * @return bool
     */
    protected function match(string $key)
    {
        if ($this->pattern === '*' and empty($this->excludes)) {
            return true;
        }

        if (!$this->rdb->match($this->pattern, $key)) {
            return false;
        }

        foreach ($this->excludes as $exclude) {
            if ($this->rdb->match($exclude, $key)) {
                return false;
            }
        }

        return true;
    }


    /**
     * Open a file in the given 'compression' mode.
     *
     * Note, gzip will auto-append the binary mode.
     *
     * TODO add gzip compression 1-9 support.
     *
     * @param string|resource $file
     * @param string $mode
     * @return void
     * @throws Exception if the file cannot be opened.
     */
    protected function open($file, string $mode)
    {
        if (is_string($file)) {
            if ($this->compressed) {
                $handle = @gzopen($file, $mode . 'b');
            }
            else {
                $handle = @fopen($file, $mode);
            }

            if (!$handle) {
                throw new Exception("Failed to open file: {$file}");
            }

            $this->handle = $handle;
        }
        else {
            if (!is_resource($file)) {
                throw new Exception("Invalid file handle");
            }

            $this->handle = $file;
        }
    }


    /**
     * Is this file a gzip?
     *
     * @param string $path
     * @return bool
     */
    public static function isGzip(string $path): bool
    {
        $file = @fopen($path, 'rb');

        if (!$file) {
            return false;
        }

        try {
            $bytes = fread($file, 2);

            if (strlen($bytes) != 2) {
                return false;
            }

            [, $b1, $b2] = unpack('C2', $bytes);

            if ($b1 != 0x1f or $b2 != 0x8b) {
                return false;
            }

            return true;
        }
        finally {
            fclose($file);
        }
    }


    /**
     * Get all the lines from the file.
     *
     * @return iterable<string|false>
     */
    protected function gets()
    {
        if ($this->compressed) {
            $gets = 'gzgets';
            $eof = 'gzeof';
        }
        else {
            $gets = 'fgets';
            $eof = 'feof';
        }

        for (;;) {
            $value = $gets($this->handle);

            if ($eof($this->handle)) {
                if ($value) {
                    yield $value;
                }

                break;
            }

            yield $value;
        }
    }


    /**
     * Is this file open?
     *
     * @return bool false if open, true if closed
     */
    protected function eof(): bool
    {
        if ($this->compressed) {
            return gzeof($this->handle);
        }
        else {
            return feof($this->handle);
        }
    }


    /**
     * Close ths file.
     *
     * Warning: only close your own files!
     *
     * @return void
     */
    protected function close()
    {
        if ($this->compressed) {
            gzclose($this->handle);
        }
        else {
            fclose($this->handle);
        }
    }

}
