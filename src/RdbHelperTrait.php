<?php
namespace karmabunny\rdb;

trait RdbHelperTrait
{

    /**
     * Flatten an array input.
     *
     * This also discards keys.
     *
     * @param iterable $items
     * @return array
     * @deprecated use flatten()
     */
    protected static function flattenArrays(iterable $items): array
    {
        return self::flatten($items);
    }


    /**
     * Normalize values into an array.
     *
     * @param iterable $items
     * @param bool $preserve_keys ignored
     * @return array
     * @deprecated use flatten()
     */
    protected static function normalizeIterable($items, $preserve_keys = false): array
    {
        return self::flatten($items);
    }



    /**
     * Flatten an array input.
     *
     * This also discards keys.
     *
     * Largely ripped from karmabunny/kb.
     *
     * @param iterable $items
     * @param int $depth
     * @return array
     */
    protected static function flatten(iterable $items, int $depth = 25): array
    {
        $output = [];

        foreach ($items as $value) {
            if (is_iterable($value)) {
                if ($depth <= 1) {
                    continue;
                }

                $value = self::flatten($value, $depth - 1);
                foreach ($value as $sub) {
                    $output[] = $sub;
                }
            }
            else {
                $output[] = $value;
            }
        }

        return $output;
    }


    /**
     * Flatten the keys of an array with a 'glue'.
     *
     * For example:
     * ```
     * [ 'abc' => [
     *     'def' => 123,
     *     'ghi' => 567,
     * ]]
     * ```
     *
     * Becomes:
     * ```
     * [
     *     'abc.def' => 123,
     *     'abc.ghi' => 567,
     * ]
     * ```
     *
     * @param iterable $item
     * @param string $glue
     * @return array
     */
    protected static function flattenKeys(iterable $item, string $glue = '.'): array
    {
        $flat = [];

        foreach ($item as $key => $value) {
            if (is_iterable($value)) {

                // Recurse in!
                $subflat = self::flattenKeys($value, $glue);

                foreach ($subflat as $subkey => $subvalue) {
                    $flat[$key . $glue . $subkey] = $subvalue;
                }
            }
            else {
                $flat[$key] = $value;
            }
        }

        return $flat;
    }


    /**
     * Convert a flattened array into a nested form.
     *
     * For example:
     * ```
     * [
     *     'abc.def' => 123,
     *     'abc.ghi.0' => 456,
     *     'abc.ghi.1' => 789,
     * ]
     * ```
     *
     * Becomes:
     * ```
     * [ 'abc' => [
     *     'def' => 123,
     *     'ghi' => [456, 789],
     * ]]
     * ```
     *
     * @param iterable $item
     * @param string $glue
     * @return array
     */
    protected static function explodeFlatKeys(iterable $item, string $glue = '.'): array
    {
        $output = [];

        foreach ($item as $key => $value) {
            $parts = explode($glue, $key);
            $target = &$output;

            // Creates create the whole path as a deep nested array.
            // abc.def.ghi => [abc][def][ghi] = []
            foreach ($parts as $part) {
                // @phpstan-ignore-next-line
                if (!isset($target[$part])) {
                    $target[$part] = [];
                }

                // Iterate into the next level.
                $target = &$target[$part];
            }

            // Target is now the deepest level of the path.
            $target = $value;
        }

        return $output;
    }


    /**
     * Parse + normalise set() flags.
     *
     * @param array $flags
     * @return array
     */
    protected static function parseSetFlags(array $flags): array
    {
        // Defaults.
        $output = [
            'keep_ttl' => null,
            'time_at' => null,
            'get_set' => null,
            'replace' => null,
        ];

        // Normalise.
        foreach ($flags as $key => $value) {
            if (is_numeric($key)) {
                $output[strtolower($value)] = true;
            }
            else {
                $output[strtolower($key)] = $value;
            }
        }

        // For those smarty pants that write 'replace => NX/XX'.
        if ($output['replace'] === 'NX') {
            $output['replace'] = false;
        }
        else if ($output['replace'] === 'XX') {
            $output['replace'] = true;
        }
        else if (!is_bool($output['replace'])) {
            $output['replace'] = null;
        }

        return $output;
    }


    /**
     * Parse + normalise zrange() flags.
     *
     * @param array $flags
     * @return array
     */
    protected static function parseRangeFlags(array $flags): array
    {
        $output = [
            'withscores' => false,
            'byscore' => false,
            'bylex' => false,
            'rev' => false,
            'limit' => null,
        ];

        // Normalise things.
        foreach ($flags as $key => $value) {
            if (is_numeric($key)) {
                $flags[strtolower($value)] = true;
            }
            else {
                $flags[strtolower($key)] = $value;
            }
        }

        if (!empty($flags['with_scores']) or !empty($flags['withscores'])) {
            $output['withscores'] = true;
        }

        if (!empty($flags['by_score']) or !empty($flags['byscore'])) {
            $output['byscore'] = true;
        }

        if (!empty($flags['by_lex']) or !empty($flags['bylex'])) {
            $output['bylex'] = true;
        }

        if (!empty($flags['reverse']) or !empty($flags['rev'])) {
            $output['rev'] = true;
        }

        // Parse limit into a keyed array.
        if (
            !empty($flags['limit'])
            and is_array($flags['limit'])
            and count($flags['limit']) >= 2
        ) {
            $offset = 0;
            $count = -1;

            // Numeric version.
            if (isset($flags['limit'][0]) and isset($flags['limit'][1])) {
                [$offset, $count] = $flags['limit'];
            }

            // Keyed version.
            if (isset($flags['limit']['offset'])) {
                $offset = $flags['limit']['offset'];
            }

            if (isset($flags['limit']['count'])) {
                $count = $flags['limit']['count'];
            }

            $output['limit'] = [
                'offset' => $offset,
                'count' => $count,
            ];
        }

        return $output;
    }


    /**
     *
     * @param array $flags
     * @return array
     */
    protected static function parseRestoreFlags(array $flags): array
    {
        // Defaults.
        $output = [
            'replace' => false,
            'absttl' => false,
            'idletime' => null,
            'freq' => null,
        ];

        // Normalise things.
        foreach ($flags as $key => $value) {
            if (is_numeric($key)) {
                $flags[strtolower($value)] = true;
            }
            else {
                $flags[strtolower($key)] = $value;
            }
        }

        if ($flags['replace'] ?? false) {
            $output['replace'] = true;
        }

        // TODO other flags.

        return $output;
    }
}