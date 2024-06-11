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
        $this->assertEmpty(array_diff($expected, $actual));
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


    public function testAppend()
    {
        // No exist.
        $thing = $this->rdb->get('string:append');
        $this->assertNull($thing);

        // append to empty, creates a record.
        $expected = 'abc';
        $length = $this->rdb->append('string:append', $expected);
        $this->assertEquals(strlen($expected), $length);

        // append some more.
        $expected .= '-more';
        $length = $this->rdb->append('string:append', '-more');
        $this->assertEquals(strlen($expected), $length);

        $actual = $this->rdb->get('string:append');
        $this->assertEquals($expected, $actual);
    }


    public function testGetRange()
    {
        // No exist.
        $thing = $this->rdb->get('string:range');
        $this->assertNull($thing);

        $string = 'one:two:333';

        // Sample data.
        $ok = $this->rdb->set('string:range', $string);
        $this->assertTrue($ok);

        // Whole string with defaults.
        $expected = $string;
        $actual = $this->rdb->getRange('string:range');
        $this->assertEquals($expected, $actual);

        $actual = $this->rdb->substr('string:range', 0);
        $this->assertEquals($expected, $actual);

        // Offsets.
        $expected = substr($string, 4);
        $actual = $this->rdb->getRange('string:range', 4);
        $this->assertEquals($expected, $actual);

        $actual = $this->rdb->substr('string:range', 4);
        $this->assertEquals($expected, $actual);

        // Offsets and lengths.
        $expected = substr($string, 4, 3);
        $actual = $this->rdb->getRange('string:range', 4, 6);
        $this->assertEquals($expected, $actual);

        $actual = $this->rdb->substr('string:range', 4, 3);
        $this->assertEquals($expected, $actual);

        // Negative lengths.
        $expected = substr($string, 4, -4);
        $actual = $this->rdb->getRange('string:range', 4, -5);
        $this->assertEquals($expected, $actual);

        $actual = $this->rdb->substr('string:range', 4, -4);
        $this->assertEquals($expected, $actual);
    }


    public function testSetExpire()
    {
    }


    public function testMultiSetGet()
    {
    }


    public function testSets()
    {
        $expected = [];
        $actual = $this->rdb->sMembers('yes:a:set');
        $this->assertArraySameAs($expected, $actual);

        // Adding to a set.
        $expected = 2;
        $actual = $this->rdb->sAdd('yes:a:set', 'abc', 'def');
        $this->assertEquals($expected, $actual);

        $expected = 0;
        $actual = $this->rdb->sAdd('yes:a:set', 'def');
        $this->assertEquals($expected, $actual);

        $expected = 1;
        $actual = $this->rdb->sAdd('yes:a:set', 'def', 'ghi');
        $this->assertEquals($expected, $actual);

        // Check.
        $expected = ['abc', 'def', 'ghi'];
        $actual = $this->rdb->sMembers('yes:a:set');
        $this->assertArraySameAs($expected, $actual);

        $actual = $this->rdb->sScan('yes:a:set');
        $actual = iterator_to_array($actual);
        $this->assertArraySameAs($expected, $actual);

        // Removing from a set.
        $expected = 0;
        $actual = $this->rdb->sRem('yes:a:set', 'blah');
        $this->assertEquals($expected, $actual);

        $expected = 1;
        $actual = $this->rdb->sRem('yes:a:set', 'def', 'blah');
        $this->assertEquals($expected, $actual);

        $expected = 2;
        $actual = $this->rdb->sRem('yes:a:set', 'abc', 'ghi');
        $this->assertEquals($expected, $actual);

        // Check.
        $expected = [];
        $actual = $this->rdb->sMembers('yes:a:set');
        $this->assertArraySameAs($expected, $actual);

        $actual = $this->rdb->sScan('yes:a:set');
        $actual = iterator_to_array($actual);
        $this->assertArraySameAs($expected, $actual);

        // Testing bad types.
        $this->rdb->lPush('not:a:set', 'abc');
        $actual = $this->rdb->type('not:a:set');
        $this->assertEquals('list', $actual);

        // Push set onto list, should fail.
        $actual = $this->rdb->sAdd('not:a:set', 'abc');
        $this->assertNull($actual);

        // Get members from a list, should fail.
        $actual = $this->rdb->sMembers('not:a:set');
        $this->assertNull($actual);

        // Test members on a list, should fail.
        $actual = $this->rdb->sIsMember('not:a:set', 'abc');
        $this->assertNull($actual);

        // Test cardinality on a list, should fail.
        $actual = $this->rdb->sCard('not:a:set');
        $this->assertNull($actual);

        // // Remove from from a list, should fail.
        $actual = $this->rdb->sRem('not:a:set', 'abc');
        $this->assertNull($actual);

        // Test move on a list to a set, should fail.
        $actual = $this->rdb->sMove('not:a:set', 'yes:a:set', 'abc');
        $this->assertNull($actual);

        // Test move on a list to a set, a bit different.
        $actual = $this->rdb->sMove('yes:a:set', 'not:a:set', 'abc');
        $this->assertFalse($actual);

        // But if the set _HAS_ values, not-a-set errors come after.
        $this->rdb->sAdd('yes:a:set', 'abc', 'def');

        $actual = $this->rdb->sMove('yes:a:set', 'not:a:set', 'abc');
        $this->assertNull($actual);
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


    public function testIncrementBy()
    {
        // No exist.
        $thing = $this->rdb->get('my:value');
        $this->assertNull($thing);

        $new = $this->rdb->incrBy('my:value', 1);
        $this->assertEquals(1, $new);

        $new = $this->rdb->incrBy('my:value', 1);
        $this->assertEquals(2, $new);

        $new = $this->rdb->incrBy('my:value', 2);
        $this->assertEquals(4, $new);

        $new = $this->rdb->incrBy('my:other:value', 1);
        $this->assertEquals(1, $new);

        $new = $this->rdb->decrBy('my:value', 1);
        $this->assertEquals(3, $new);

        $new = $this->rdb->decrBy('my:value', 3);
        $this->assertEquals(0, $new);

        $actual = $this->rdb->get('my:value');
        $expected = 0;

        $this->assertEquals($expected, $actual);

        $actual = $this->rdb->get('my:other:value');
        $expected = 1;

        $this->assertEquals($expected, $actual);
    }


    public function testIncrementByFloat()
    {
        // No exist.
        $thing = $this->rdb->get('my:value');
        $this->assertNull($thing);

        $new = $this->rdb->incrByFloat('my:value', 0.5);
        $this->assertEquals(0.5, $new);

        $new = $this->rdb->incrByFloat('my:value', 1.5);
        $this->assertEquals(2, $new);

        $new = $this->rdb->incrByFloat('my:value', 0.87);
        $this->assertEquals(2.87, $new);

        $new = $this->rdb->incrByFloat('my:other:value', 1.258);
        $this->assertEquals(1.258, $new);

        $actual = $this->rdb->get('my:value');
        $expected = 2.87;

        $this->assertEquals($expected, $actual);

        $actual = $this->rdb->get('my:other:value');
        $expected = 1.258;

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



    public function testSortedSets()
    {

        // zAdd.
        $actual = $this->rdb->zAdd('zrange:123', ['a' => 1]);
        $this->assertEquals(1, $actual);

        // zIncrby.
        $actual = $this->rdb->zIncrBy('zrange:123', 3, 'a');
        $this->assertEquals(4, $actual);

        // zRange.
        $actual = $this->rdb->zRange('zrange:123', 0, -1, ['withscores' => true]);
        $this->assertEquals(['a' => 4], $actual);

        // zRem.
        $actual = $this->rdb->zRem('zrange:123', 'a');
        $this->assertEquals(1, $actual);

        // Null sets.
        $this->rdb->set('zrange:hello', 'not-a-sorted-set');
        $actual = $this->rdb->zRange('zrange:hello');
        $this->assertNull($actual);

        // missing set.
        $this->rdb->del('zrange:hello');
        $actual = $this->rdb->zRange('zrange:hello');
        $this->assertEquals([], $actual);

        // zIncrBy creates a set.
        $this->rdb->del('zrange:hello');
        $actual = $this->rdb->zIncrBy('zrange:hello', 10, 'a');
        $this->assertEquals(10, $actual);

        $actual = $this->rdb->zRange('zrange:hello');
        $this->assertEquals(['a'], $actual);

        // zAdd, multiple.
        $expected = 6;
        $actual = $this->rdb->zAdd('zrange:123', [
            'a' => 10,
            'b' => 3,
            'c' => 5,
            'e' => 5,
            'd' => 5,
            'f' => 5,
        ]);
        $this->assertEquals($expected, $actual);

        // zRange, no scores - but results are ordered.
        $expected = ['b', 'c', 'd', 'e', 'f', 'a'];
        $actual = $this->rdb->zRange('zrange:123');
        $this->assertEquals($expected, $actual);

        // zRange, no scores, just the first.
        $expected = ['b'];
        $actual = $this->rdb->zRange('zrange:123', 0, 0);
        $this->assertEquals($expected, $actual);

        // zRange, with scores, just the first.
        $expected = [ 'b' => 3 ];
        $actual = $this->rdb->zRange('zrange:123', 0, 0, ['withscores' => true]);
        $this->assertEquals($expected, $actual);

        // zRange, with scores, truncated.
        $expected = [
            'b' => 3,
            'c' => 5,
        ];
        $actual = $this->rdb->zRange('zrange:123', 0, 1, ['withscores' => true]);
        $this->assertEquals($expected, $actual);

        // zRange, by lex (with defaults).
        $expected = [ 'b', 'c', 'd', 'e', 'f', 'a' ];
        $actual = $this->rdb->zRange('zrange:123', null, null, ['bylex']);
        $this->assertEquals($expected, $actual);

        // zRange, by lex with inclusive defaults.
        $expected = [ 'b', 'c', 'd' ];
        $actual = $this->rdb->zRange('zrange:123', 'a', 'd', ['bylex']);
        $this->assertEquals($expected, $actual);

        // zRange, by lex with exclusive options.
        $expected = [ 'b', 'c' ];
        $actual = $this->rdb->zRange('zrange:123', '-', '(d', ['bylex']);
        $this->assertEquals($expected, $actual);

        // zRange, by score (defaults).
        $expected = [ 'b', 'c', 'd', 'e', 'f', 'a' ];
        $actual = $this->rdb->zRange('zrange:123', null, null, ['byscore']);
        $this->assertEquals($expected, $actual);

        // zRange, with filters.
        $expected = [];
        $actual = $this->rdb->zRange('zrange:123', 0, 1, ['byscore']);
        $this->assertEquals($expected, $actual);

        // zRange, with proper filters.
        $expected = ['b', 'c', 'd', 'e', 'f'];
        $actual = $this->rdb->zRange('zrange:123', 3, 5, ['byscore']);
        $this->assertEquals($expected, $actual);

        // Testing: zCard, zCount, zScore, zRank, zRevRank.
        $this->rdb->zAdd('ztest', [
            'a' => 1,
            'c' => 10,
            'b' => 5,
        ]);

        $expected = 3;
        $actual = $this->rdb->zCard('ztest');
        $this->assertEquals($expected, $actual);

        $expected = 3;
        $actual = $this->rdb->zCount('ztest', 0, INF);
        $this->assertEquals($expected, $actual);

        $expected = 2;
        $actual = $this->rdb->zCount('ztest', 5, INF);
        $this->assertEquals($expected, $actual);

        $expected = 1;
        $actual = $this->rdb->zCount('ztest', 6, INF);
        $this->assertEquals($expected, $actual);

        $expected = 1;
        $actual = $this->rdb->zCount('ztest', 3, 7);
        $this->assertEquals($expected, $actual);

        $expected = 10;
        $actual = $this->rdb->zScore('ztest', 'c');
        $this->assertEquals($expected, $actual);

        $expected = 2;
        $actual = $this->rdb->zRank('ztest', 'c');
        $this->assertEquals($expected, $actual);

        $expected = 1;
        $actual = $this->rdb->zRank('ztest', 'b');
        $this->assertEquals($expected, $actual);

        $expected = 0;
        $actual = $this->rdb->zRevRank('ztest', 'c');
        $this->assertEquals($expected, $actual);

        $expected = 1;
        $actual = $this->rdb->zRevRank('ztest', 'b');
        $this->assertEquals($expected, $actual);
    }



    public function testSelectMove()
    {
        // Quick tidy up.
        $this->rdb->select(1);
        $this->rdb->del($this->rdb->keys('*'));
        $this->rdb->select(0);

        $expected = 'hello';
        $this->rdb->set('move:thing', $expected);

        $actual = $this->rdb->get('move:thing');
        $this->assertEquals($expected, $actual);

        // Do the move.
        $this->rdb->move('move:thing', 1);

        // Check empty.
        $actual = $this->rdb->get('move:thing');
        $this->assertNull($actual);

        // Switch and check new DB location.
        $this->rdb->select(1);

        $actual = $this->rdb->get('move:thing');
        $this->assertEquals($expected, $actual);
    }


    public function dataDump()
    {
        // command - expected
        return [
            [ 'set', 'one' ],
            [ 'rPush', ['x', 'a', 'z'] ],
            [ 'sAdd', ['a', 'z', 'b'] ],
            [ 'zAdd', ['a' => 10, 'g' => 5, 'z' => 7] ],
        ];
    }


    /**
     * @dataProvider dataDump
     */
    public function testDumpRestore($command, $expected)
    {
        $this->rdb->$command('dump:restore', $expected);

        $dump = $this->rdb->dump('dump:restore');
        $this->assertNotNull($dump);

        $ok = $this->rdb->del('dump:restore');
        $this->assertEquals(1, $ok);

        $ok = $this->rdb->restore('dump:restore', 0, $dump, ['replace']);
        $this->assertTrue($ok);

        switch ($command) {
            case 'set':
                $actual = $this->rdb->get('dump:restore');
                break;

            case 'rPush':
                $actual = $this->rdb->lRange('dump:restore');
                break;

            case 'sAdd':
                $actual = $this->rdb->sMembers('dump:restore');
                sort($actual);
                sort($expected);
                break;

            case 'zAdd':
                $actual = $this->rdb->zRange('dump:restore', null, null, ['withscores']);
                break;

            default:
                $this->fail('Unknown command: ' . $command);
        }

        $this->assertEquals($expected, $actual);
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