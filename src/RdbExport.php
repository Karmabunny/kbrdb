<?php
/**
 * @link      https://github.com/Karmabunny
 * @copyright Copyright (c) 2021 Karmabunny
 */

namespace karmabunny\rdb;

use Exception;
use Generator;

/**
 * Bulk export of data.
 *
 * This uses the 'dump' command. Types and TTLs are preserved.
 *
 * The output format is a JSON-lines file, with base64 encoded values.
 *
 * By default the file will be gzip compressed.
 *
 * ```
 * [ key1, ttl, base64 ]\n
 * [ key2, ttl, base64 ]\n
 * [ key3, ttl, base64 ]\n
 * ...
 * ```
 *
 * Attach a 'log' callback to track export progress and errors.
 *
 * @see RdbImport
 * @package karmabunny\rdb
 */
class RdbExport
{
    use RdbDumpTrait;


    /**
     * Write an entry to the file.
     *
     * This encodes and zips (if enabled).
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
     * Read all data from the DB with the given 'pattern'.
     *
     * This returns a 1-index key.
     *
     * @return Generator<int, array> [ key, ttl, value ]
     */
    public function read(): Generator
    {
        $keys = $this->rdb->scan($this->pattern);
        $index = 0;

        foreach ($keys as $key) {
            if (!$this->match($key)) {
                continue;
            }

            $index++;
            $value = $this->rdb->dump($key);

            $ttl = $this->rdb->ttl($key) ?? 0;
            $ttl = max(0, $ttl);

            yield $index => [$key, $ttl, $value];
        }
    }


    /**
     * Export all data from the DB with the 'pattern' into this file.
     *
     * If providing a file handle, please ensure the type matches
     * the 'compressed' setting. Handles are not closed after writing.
     *
     * @param string|resource $file
     * @return int The number of items exported.
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

        return $index;
    }
}
