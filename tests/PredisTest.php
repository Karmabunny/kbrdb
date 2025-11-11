<?php

namespace kbtests;

use karmabunny\rdb\Objects\PhpObjectDriver;
use karmabunny\rdb\Rdb;
use karmabunny\rdb\RdbConfig;
use PHPUnit\Framework\TestCase;

/**
 * Test the predis adapter.
 */
final class PredisTest extends TestCase
{
    use AdapterTestTrait;
    use DumpTestTrait;
    use BucketTestTrait;
    use LockingTestTrait;


    /** @var Rdb */
    public $rdb;


    public function createRdb(): Rdb
    {
        return Rdb::create([
            'prefix' => uniqid('rdb:') . ':',
            'adapter' => RdbConfig::TYPE_PREDIS,
        ]);
    }


    public function setUp(): void
    {
        $this->rdb ??= $this->createRdb();
        $this->rdb->select(0);
        $this->rdb->del($this->rdb->keys('*'));
    }


    public function tearDown(): void
    {
        $this->rdb->driver = PhpObjectDriver::class;
    }

}
