<?php

namespace kbtests;

use karmabunny\rdb\Rdb;
use PHPUnit\Framework\TestCase;

/**
 * Tests for adapters.
 *
 * Extend this and swap in a different adapter config.
 */
abstract class AdapterTestCase extends TestCase
{
    /** @var Rdb */
    public $rdb;


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


    public function testSetExpire()
    {
    }


    public function testMultiSetGet()
    {
    }


    public function testSets()
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


    public function testKeysExists()
    {
    }


    public function testScan()
    {
    }
}
