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
class RdbImport
{
    use RdbDumpTrait;

    /** @var string[] */
    public $errors = [];


    /**
     *
     * @param string|resource $file
     * @return iterable<array> [ key, type, data ]
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
            if (
                $this->pattern !== '*'
                and $this->rdb->match($this->pattern, $key)
            ) {
                continue;
            }

            $value = base64_decode($value);

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
     *
     * @param string|resource $file
     * @return bool
     * @throws Exception
     */
    public function import($file): bool
    {
        $this->errors = [];

        $data = $this->read($file);

        foreach ($data as $index => $item) {
            [$key, $ttl, $value] = $item;

            $this->rdb->restore($key, $ttl, $value, ['replace']);

            if ($this->log) {
                ($this->log)($index);
            }
        }

        return empty($this->errors);
    }
}
