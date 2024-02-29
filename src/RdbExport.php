<?php
/**
 * @link      https://github.com/Karmabunny
 * @copyright Copyright (c) 2021 Karmabunny
 */

namespace karmabunny\rdb;

use Exception;
use Generator;

/**
 * Dump stuff.
 *
 * @package karmabunny\rdb
 */
class RdbExport
{
    use RdbDumpTrait;


    /**
     *
     * @param string $key
     * @param int $ttl
     * @param string $value
     * @return void
     */
    protected function write(string $key, int $ttl, string $value)
    {
        $value = base64_encode($value);
        $json = json_encode([$key, $ttl, $value]) . "\n";

        if ($this->compressed) {
            gzwrite($this->handle, $json);
        }
        else {
            fwrite($this->handle, $json);
        }
    }


    /**
     *
     * @return iterable<array> [ key, type, data ]
     */
    public function read(): Generator
    {
        $keys = $this->rdb->scan($this->pattern);
        $index = 0;

        foreach ($keys as $key) {
            $index++;
            $value = $this->rdb->dump($key);

            $ttl = $this->rdb->ttl($key) ?? 0;
            $ttl = max(0, $ttl);

            yield $index => [$key, $ttl, $value];
        }
    }


    /**
     *
     * @param string|resource $file
     * @return void
     * @throws Exception
     */
    public function export($file)
    {
        $this->open($file, 'w');

        $index = 0;
        $data = $this->read();

        foreach ($data as $index => $item) {
            [$key, $ttl, $value] = $item;

            $this->write($key, $ttl, $value);

            if ($this->log) {
                ($this->log)($index);
            }
        }

        // Only close if it's one of ours.
        if (is_string($file)) {
            $this->close();
        }
    }
}
