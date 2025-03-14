<?php

namespace kbtests;

use karmabunny\rdb\Rdb;
use karmabunny\rdb\RdbExport;
use karmabunny\rdb\RdbImport;
use PHPUnit\Framework\TestCase;

/**
 * Test the predis adapter.
 *
 * @mixin TestCase
 * @property Rdb $rdb
 */
trait DumpTestTrait
{


    public function random($length)
    {
        static $chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz1234567890';
        $bytes = random_bytes($length);
        $size = strlen($chars);

        $bytes = str_split($bytes);

        foreach ($bytes as &$byte) {
            $byte = $chars[ord($byte) % $size];
        }

        unset($byte);
        return implode('', $bytes);
    }


    public function dataDump()
    {
        $data = [];

        // String data.
        foreach (range(0, 99) as $i) {
            $string = $this->random(100);
            $key = 'string:' . substr($string, 0, 10);
            $data['string'][$key] = substr($string, 10);
        }

        // List data.
        foreach (range(0, 99) as $i) {
            $list = $this->random(100);
            $list = str_split($list, 10);

            $key = 'list:' . $this->random(10);
            $data['list'][$key] = $list;
        }

        // Sets.
        foreach (range(0, 99) as $i) {
            $list = $this->random(100);
            $list = str_split($list, 10);

            $key = 'set:' . $this->random(10);
            $data['set'][$key] = $list;
        }

        // Sorted sets.
        foreach (range(0, 99) as $i) {
            $list = $this->random(100);
            $list = str_split($list, 10);

            uasort($list, function() {
                return random_int(-1, 1);
            });

            $list = array_flip($list);

            $key = 'zset:' . $this->random(10);
            $data['zset'][$key] = $list;
        }

        // Hash data.
        foreach (range(0, 99) as $i) {
            $hash = $this->random(100);
            $hash = str_split($hash, 10);
            $hash = array_combine(
                array_map(fn() => $this->random(10), $hash),
                $hash,
            );

            $key = 'hash:' . $this->random(10);
            $data['hash'][$key] = $hash;
        }

        return [
            'compressed' => [$data, true],
            'uncompressed' => [$data, false],
        ];
    }


    public function insert($data)
    {
        foreach ($data['string'] as $key => $value) {
            $this->rdb->set($key, $value);
        }

        foreach ($data['list'] as $key => $list) {
            $this->rdb->lPush($key, ...$list);
        }

        foreach ($data['set'] as $key => $set) {
            $this->rdb->sAdd($key, ...$set);
        }

        foreach ($data['zset'] as $key => $zset) {
            $this->rdb->zAdd($key, $zset);
        }

        foreach ($data['hash'] as $key => $hash) {
            $this->rdb->hmSet($key, $hash);
        }
    }


    /**
     * @dataProvider dataDump
     */
    public function testExport($data, $compressed)
    {
        $this->insert($data);

        // We've got some test data.
        $keys = $this->rdb->keys('*');
        $this->assertNotEmpty($keys);

        $name = $compressed ? 'compressed' : 'uncompressed';
        $path = __DIR__ . "/data/export-{$name}.rdb";

        // Export it.
        $export = new RdbExport($this->rdb);
        $export->compressed = $compressed;
        $export->export($path);

        // Load the importer for reading.
        $import = new RdbImport($this->rdb);
        $import->compressed = $compressed;

        $data = $import->read($path);
        $data = iterator_to_array($data);

        // Sanity checks.
        $this->assertEmpty($import->errors, json_encode($import->errors));

        // Confirm size.
        $expected = count($data);
        $actual = count($keys);
        $this->assertEquals($expected, $actual);

        // Compare data.
        // These are exact comparisons because we haven't reinserted anything.
        foreach ($data as $item) {
            [$key, $ttl, $actual] = $item;

            $expected = $this->rdb->dump($key);
            $this->assertEquals($expected, $actual);
        }

        $this->rdb->del($keys);

        // Empty again.
        $empty = $this->rdb->keys('*');
        $this->assertEmpty($empty);

        // Run a full import.
        $import->import($path);

        // Sanity checks.
        $this->assertEmpty($import->errors, json_encode($import->errors));

        // Confirm size.
        $newKeys = $this->rdb->keys('*');

        $expected = count($keys);
        $actual = count($newKeys);
        $this->assertEquals($expected, $actual);
    }


    /**
     * @dataProvider dataDump
     */
    public function testImport($data, $compressed)
    {
        $this->insert($data);

        // We've got some test data.
        $keys = $this->rdb->keys('*');
        $this->assertNotEmpty($keys);

        $name = $compressed ? 'compressed' : 'uncompressed';
        $path = __DIR__ . "/data/import-{$name}.rdb";

        // Export it.
        $export = new RdbExport($this->rdb);
        $export->compressed = $compressed;
        $export->export($path);

        // Import it.
        $import = new RdbImport($this->rdb);
        $import->compressed = $compressed;
        $import->import($path);

        $keys = $this->rdb->scan('*');

        // Compare real data.
        foreach ($keys as $key) {
            $type = $this->rdb->type($key);
            $expected = $data[$type][$key] ?? null;

            $this->assertNotNull($expected);

            switch ($type) {
                case 'string':
                    $actual = $this->rdb->get($key);
                    $this->assertEquals($expected, $actual, $type);
                    break;

                case 'list':
                    $actual = $this->rdb->lRange($key, 0, -1);
                    sort($actual);
                    sort($expected);
                    $this->assertEquals($expected, $actual, $type);
                    break;

                case 'set':
                    $actual = $this->rdb->sMembers($key);
                    sort($actual);
                    sort($expected);
                    $this->assertEquals($expected, $actual, $type);
                    break;

                case 'zset':
                    $actual = $this->rdb->zRange($key, 0, -1, ['withscores']);
                    $this->assertEquals($expected, $actual, $type);
                    break;

                case 'hash':
                    $actual = $this->rdb->hGetAll($key);
                    $this->assertEquals($expected, $actual, $type);
                    break;
            }
        }
    }


    public function testPatternExport()
    {
        $data = $this->dataDump();
        [$data] = reset($data);

        $path = __DIR__ . '/data/export-patterns.rdb';

        // Clean initial.
        $keys = $this->rdb->keys('*');
        $this->assertEmpty($keys);

        $this->insert($data);

        // Lots of data.
        $keys = $this->rdb->keys('*');
        $this->assertNotEmpty($keys);

        $this->rdb->export($path, 'string:*');

        // Remove it all.
        $keys = $this->rdb->keys('*');
        $this->rdb->del($keys);

        // Clean again.
        $keys = $this->rdb->keys('*');
        $this->assertEmpty($keys);

        // Import it back.
        $errors = $this->rdb->import($path);
        $this->assertEmpty($errors, json_encode($errors));

        // We have keys again.
        $keys = $this->rdb->keys('*');
        $this->assertNotEmpty($keys);

        // We have the same count.
        $expected = count($data['string']);
        $this->assertCount($expected, $keys);

        // We have the same data.
        foreach ($data['string'] as $key => $expected) {
            $actual = $this->rdb->get($key);
            $this->assertEquals($expected, $actual);
        }
    }


    public function testPatternImport()
    {
        $data = $this->dataDump();
        [$data] = reset($data);

        $path = __DIR__ . '/data/import-patterns.rdb';

        // Clean initial.
        $keys = $this->rdb->keys('*');
        $this->assertEmpty($keys);

        $this->insert($data);

        // Lots of data.
        $keys = $this->rdb->keys('*');
        $this->assertNotEmpty($keys);

        // Export it all.
        $this->rdb->export($path);

        // Remove it all.
        $keys = $this->rdb->keys('*');
        $this->rdb->del($keys);

        // Clean again.
        $keys = $this->rdb->keys('*');
        $this->assertEmpty($keys);

        // Import it back, just strings though.
        $errors = $this->rdb->import($path, 'string:*');
        $this->assertEmpty($errors, json_encode($errors));

        // We have keys again.
        $keys = $this->rdb->keys('*');
        $this->assertNotEmpty($keys);

        // We have the same count.
        $expected = count($data['string']);
        $this->assertCount($expected, $keys, json_encode($keys));

        // We have the same data.
        foreach ($data['string'] as $key => $expected) {
            $actual = $this->rdb->get($key);
            $this->assertEquals($expected, $actual);
        }
    }

    public function testPatternExclude()
    {
        $data = $this->dataDump();
        [$data] = reset($data);

        $path = __DIR__ . '/data/exclude-patterns.rdb';

        // Clean initial.
        $keys = $this->rdb->keys('*');
        $this->assertEmpty($keys);

        $this->insert($data);

        // Lots of data.
        $keys = $this->rdb->keys('*');
        $this->assertNotEmpty($keys);

        // Export some of it.
        $export = new RdbExport($this->rdb);
        $export->setExclude(['set:*', 'zset:*']);
        $export->export($path);

        // Remove it all.
        $keys = $this->rdb->keys('*');
        $this->rdb->del($keys);

        // Clean again.
        $keys = $this->rdb->keys('*');
        $this->assertEmpty($keys);

        // Import it back, just strings though.
        $errors = $this->rdb->import($path, 'string:*');
        $this->assertEmpty($errors, json_encode($errors));

        // We have keys again.
        $keys = $this->rdb->keys('*');
        $this->assertNotEmpty($keys);

        // We have the same count.
        $expected = count($data['string']);
        $this->assertCount($expected, $keys, json_encode($keys));

        // We have the same data.
        foreach ($data['string'] as $key => $expected) {
            $actual = $this->rdb->get($key);
            $this->assertEquals($expected, $actual);
        }
    }


    public function testAutoCompress()
    {
        $this->rdb->set('test1', 'one');
        $this->rdb->set('test2', 'two');

        $path = __DIR__ . '/data/test-auto.rdb';

        $export = new RdbExport($this->rdb);
        $export->compressed = true;
        $export->export($path);

        // lies!
        $import = new RdbImport($this->rdb);
        $import->compressed = false;

        $items = $import->read($path);
        $count = iterator_count($items);

        $this->assertEquals(2, $count);
        $this->assertTrue($import->compressed);

        // again!
        $export = new RdbExport($this->rdb);
        $export->compressed = false;
        $export->export($path);

        $import = new RdbImport($this->rdb);
        $import->compressed = true;

        $items = $import->read($path);
        $count = iterator_count($items);

        $this->assertEquals(2, $count);
        $this->assertFalse($import->compressed);
    }
}
