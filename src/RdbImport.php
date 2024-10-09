<?php
/**
 * @link      https://github.com/Karmabunny
 * @copyright Copyright (c) 2021 Karmabunny
 */

namespace karmabunny\rdb;

use Exception;
use Generator;

/**
 * Bulk import of data.
 *
 * This uses the 'restore' command. Types and TTLs are preserved.
 *
 * The input is expected to match that of {@see RdbExport} - a JSON-lines file,
 * with base64 encoded values.
 *
 * The gzip compression is auto-detected (if using file paths).
 *
 * Attach a 'log' callback to track import progress and errors.
 *
 * @see RdbExport
 * @package karmabunny\rdb
 */
class RdbImport
{
    use RdbDumpTrait;

    /** @var string[] */
    public $errors = [];


    /**
     * Read all data from the file with the given 'pattern'.
     *
     * This returns a 1-index key.
     *
     * @param string|resource $file
     * @return Generator<int, array> [ key, type, value ]
     */
    public function read($file): Generator
    {
        if (is_string($file)) {
            $this->compressed = $this->isGzip($file);
        }

        $this->open($file, 'r');

        $index = 0;
        $data = $this->gets();

        foreach ($data as $item) {
            $index++;

            // Read error.
            if ($item === false) {
                $this->errors[$index] = 'file';
                continue;
            }

            // No error, just an empty line.
            if (!$item) {
                continue;
            }

            $item = json_decode($item, true);

            // JSON error.
            if (!is_array($item) or count($item) !== 3) {
                $this->errors[$index] = 'json';
                continue;
            }

            [$key, $ttl, $value] = $item;

            // Filter keys.
            if (!$this->match($key)) {
                continue;
            }

            $value = base64_decode($value, true);

            if ($value === false) {
                $this->errors[$index] = 'base64';
                continue;
            }

            yield $index => [$key, $ttl, $value];
        }

        // Only close if it's one of ours.
        if (is_string($file)) {
            $this->close();
        }
    }


    /**
     * Import data that matches the 'pattern' into the DB.
     *
     * If providing a file handle, please ensure the type matches
     * the 'compressed' setting. Handles are not closed after writing.
     *
     * Compressed files will be auto-detected if using a file path.
     *
     * @param string|resource $file
     * @return int The number of items imported.
     * @throws Exception
     */
    public function import($file): int
    {
        $this->errors = [];

        $index = 0;
        $data = $this->read($file);

        foreach ($data as $index => $item) {
            [$key, $ttl, $value] = $item;

            $this->rdb->restore($key, $ttl, $value, ['replace']);

            if ($this->log) {
                ($this->log)($index);
            }
        }

        return $index;
    }
}
