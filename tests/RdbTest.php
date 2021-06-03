<?php

namespace kbtests;

use karmabunny\rdb\Rdb;
use PHPUnit\Framework\TestCase;

/**
 * Test generic utilities - those that extend the base methods from the
 * adapter classes.
 */
final class RdbTest extends TestCase
{
    /** @var Rdb */
    public $rdb;

    public function setUp(): void
    {
        static $rdb;
        if (!$rdb) $rdb = Rdb::create([ 'prefix' => 'rdb:' ]);
        $this->rdb = $rdb;
    }


    public function testMultiScan()
    {
    }


    public function testObjects()
    {
    }


    public function testJson()
    {
    }
}
