<?php

namespace kbtests;

use ArrayIterator;
use karmabunny\rdb\Rdb;
use PHPUnit\Framework\TestCase;
use stdClass;
use Traversable;

/**
 * Tests for adapters.
 *
 * Extend this and swap in a different adapter config.
 */
abstract class AdapterTestCase extends TestCase
{
    /** @var Rdb */
    public $rdb;


    public function assertArraySameAs($expected, $actual)
    {
        $actual = array_intersect($expected, $actual);
        $this->assertEquals($expected, $actual);
    }


    public function testSetGet()
    {
        // No exist.
        $thing = $this->rdb->get('does:not:exist');
        $this->assertNull($thing);

        // Set.
        $expected = bin2hex(random_bytes(10));
        $ok = $this->rdb->set('thing:hi', $expected);
        $this->assertTrue($ok);

        // Get.
        $actual = $this->rdb->get('thing:hi');
        $this->assertEquals($expected, $actual);

        // Replace.
        $replace = bin2hex(random_bytes(10));
        $ok = $this->rdb->set('thing:hi', $replace);
        $actual = $this->rdb->get('thing:hi');

        $this->assertTrue($ok);
        $this->assertEquals($replace, $actual);

        // Delete.
        $num = $this->rdb->del('thing:hi');
        $this->assertEquals(1, $num);

        // To be sure.
        $thing = $this->rdb->get('thing:hi');
        $this->assertNull($thing);

        // One more time.
        $num = $this->rdb->del('thing:hi');
        $this->assertEquals(0, $num);
    }


    public function testExistsTypes()
    {
        // Empty, no type.
        $actual = $this->rdb->exists('exists:123');
        $this->assertEquals(0, $actual);

        $actual = $this->rdb->type('exists:123');
        $this->assertNull($actual);

        // String types.
        $actual = $this->rdb->exists('string:123');
        $this->assertEquals(0, $actual);

        $ok = $this->rdb->set('string:123', 1000);
        $this->assertTrue($ok);

        $actual = $this->rdb->exists('string:123');
        $this->assertEquals(1, $actual);

        $expected = 'string';
        $actual = $this->rdb->type('string:123');
        $this->assertEquals($expected, $actual);

        // Set types.
        $actual = $this->rdb->exists('set:123');
        $this->assertEquals(0, $actual);

        $ok = $this->rdb->sAdd('set:123', 'abc');
        $this->assertEquals(1, $ok);

        $actual = $this->rdb->exists('set:123');
        $this->assertEquals(1, $actual);

        $expected = 'set';
        $actual = $this->rdb->type('set:123');
        $this->assertEquals($expected, $actual);

        // List types.
        $actual = $this->rdb->exists('list:123');
        $this->assertEquals(0, $actual);

        $ok = $this->rdb->lPush('list:123', 'abc');
        $this->assertEquals(1, $ok);

        $actual = $this->rdb->exists('list:123');
        $this->assertEquals(1, $actual);

        $expected = 'list';
        $actual = $this->rdb->type('list:123');
        $this->assertEquals($expected, $actual);

        // Multi exists.
        $actual = $this->rdb->exists('string:123', 'set:123', 'list:123', 'does:not:exist');
        $this->assertEquals(3, $actual);

        // Array flattening.
        $actual = $this->rdb->exists(['string:123', 'set:123'], 'list:123');
        $this->assertEquals(3, $actual);
    }


    public function testSetExpire()
    {
    }


    public function testMultiSetGet()
    {
    }


    public function testSets()
    {
    }


    public function testLists()
    {
    }


    public function testIncrement()
    {
        // No exist.
        $thing = $this->rdb->get('my:value');
        $this->assertNull($thing);

        $new = $this->rdb->incr('my:value');
        $this->assertEquals(1, $new);

        $new = $this->rdb->incr('my:value');
        $this->assertEquals(2, $new);

        $new = $this->rdb->incr('my:value', 2);
        $this->assertEquals(4, $new);

        $new = $this->rdb->incr('my:other:value', 1);
        $this->assertEquals(1, $new);

        $new = $this->rdb->decr('my:value');
        $this->assertEquals(3, $new);

        $new = $this->rdb->decr('my:value', 3);
        $this->assertEquals(0, $new);

        $actual = $this->rdb->get('my:value');
        $expected = 0;

        $this->assertEquals($expected, $actual);

        $actual = $this->rdb->get('my:other:value');
        $expected = 1;

        $this->assertEquals($expected, $actual);
    }


    public function testScan()
    {
        $this->rdb->config->scan_keys = false;

        $expected = [];
        foreach (range(1, 100) as $i) {
            $key = 'item:' . $i;
            $expected[] = $key;
            $this->rdb->set($key, $i);
        }

        $actual = $this->rdb->keys('item:*');
        $this->assertArraySameAs($expected, $actual);

        $actual = $this->rdb->scan('item:*');
        $this->assertInstanceOf(Traversable::class, $actual);

        $actual = iterator_to_array($actual);
        $this->assertArraySameAs($expected, $actual);

        // Test fake keys().
        $this->rdb->config->scan_keys = true;

        $actual = $this->rdb->keys('item:*');
        $this->assertTrue(is_array($actual), 'Expected an array');
        $this->assertArraySameAs($expected, $actual);

        $this->rdb->config->scan_keys = false;
    }


    public function testObjects()
    {
        // get/set object
        $object = new RandoObject([ 'foo' => 123, 'bar' => 456 ]);

        $actual = $this->rdb->setObject('obj:1', $object);
        $expected = strlen(serialize($object));
        $this->assertEquals($expected, $actual);

        $exists = $this->rdb->exists('obj:1');
        $this->assertEquals(1, $exists);

        $actual = $this->rdb->getObject('obj:1');
        $this->assertEquals($object, $actual);

        $actual = $this->rdb->getObject('obj:1', RandoObject::class);
        $this->assertEquals($object, $actual);

        $actual = $this->rdb->getObject('obj:1', stdClass::class);
        $this->assertNull($actual);

        // multi get/set objects
        $objects = [
            'multi:1' => new RandoObject([ 'foo' => 123, 'bar' => 456 ]),
            'multi:2' => new RandoObject([ 'foo' => 'aaa', 'bar' => ['bbb', 'ccc'] ]),
            'multi:3' => (object) ['xyz' => 'abc', 'def' => [1,2,3,4]],
        ];

        $expected = array_map(function($item) {
            return strlen(serialize($item));
        }, $objects);

        $actual = $this->rdb->mSetObjects($objects);
        $this->assertEquals($expected, $actual);

        $keys = [
            'multi:1',
            'multi:2',
            'multi:null',
            'multi:3',
        ];

        // Fetch all, the null key is filtered out.
        $actual = $this->rdb->mGetObjects($keys);
        $this->assertEquals($objects, $actual);

        $actual = $this->rdb->mScanObjects($keys);
        $this->assertInstanceOf(Traversable::class, $actual);
        $actual = iterator_to_array($actual);
        $this->assertEquals($objects, $actual);

        // Fetch all, _including_ the null key.
        $expected = [
            'multi:1' => $objects['multi:1'],
            'multi:2' => $objects['multi:2'],
            'multi:null' => null,
            'multi:3' => $objects['multi:3'],
        ];

        $actual = $this->rdb->mGetObjects($keys, null, true);
        $this->assertEquals($expected, $actual);

        $actual = $this->rdb->mScanObjects($keys, null, true);
        $this->assertInstanceOf(Traversable::class, $actual);
        $actual = iterator_to_array($actual);
        $this->assertEquals($expected, $actual);

        // Fetch just the 'randoboject' keys.
        $expected = [
            'multi:1' => $objects['multi:1'],
            'multi:2' => $objects['multi:2'],
        ];

        $actual = $this->rdb->mGetObjects($keys, RandoObject::class);
        $this->assertEquals($expected, $actual);

        $actual = $this->rdb->mScanObjects($keys, RandoObject::class);
        $this->assertInstanceOf(Traversable::class, $actual);
        $actual = iterator_to_array($actual);
        $this->assertEquals($expected, $actual);

        // Fetch just the 'randoboject' keys, invalid/missing are null.
        $expected = [
            'multi:1' => $objects['multi:1'],
            'multi:2' => $objects['multi:2'],
            'multi:null' => null,
            'multi:3' => null,
        ];

        $actual = $this->rdb->mGetObjects($keys, RandoObject::class, true);
        $this->assertEquals($expected, $actual);

        $actual = $this->rdb->mScanObjects($keys, RandoObject::class, true);
        $this->assertInstanceOf(Traversable::class, $actual);
        $actual = iterator_to_array($actual);
        $this->assertEquals($expected, $actual);
    }


    public function testJson()
    {
    }
}


class RandoObject
{
    public $foo;
    public $bar;

    public function __construct(array $config)
    {
        foreach ($config as $key => $value) {
            $this->$key = $value;
        }
    }

    public function toArray(): array
    {
        $iterator = new ArrayIterator($this);
        return iterator_to_array($iterator);
    }
}