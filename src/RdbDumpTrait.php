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

    /** @var bool */
    public $compressed = true;

    /** @var callable|null */
    public $log = null;

    /** @var resource|null */
    protected $handle = null;


    /**
     *
     * @param Rdb $rdb
     */
    public function __construct(Rdb $rdb, string $pattern = '*')
    {
        $this->rdb = $rdb;
        $this->pattern = $pattern;
    }


    /**
     *
     * @param string $path
     * @param string $mode
     * @return void
     * @throws Exception
     */
    protected function open($path, $mode)
    {
        if (is_string($path)) {
            if ($this->compressed) {
                $handle = @gzopen($path, $mode . 'b');
            }
            else {
                $handle = @fopen($path, $mode);
            }

            if (!$handle) {
                throw new Exception("Failed to open file: {$path}");
            }
        }

        if (!is_resource($handle)) {
            throw new Exception("Invalid file handle: {$path}");
        }

        $this->handle = $handle;
    }


    /**
     * @param string $path
     * @return bool
     */
    public function isGzip(string $path): bool
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


    protected function eof()
    {
        if ($this->compressed) {
            return gzeof($this->handle);
        }
        else {
            return feof($this->handle);
        }
    }


    /**
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
