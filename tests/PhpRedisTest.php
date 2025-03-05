<?php

namespace kbtests;

use karmabunny\rdb\Objects\PhpObjectDriver;
use karmabunny\rdb\Rdb;
use karmabunny\rdb\RdbConfig;
use PHPUnit\Framework\TestCase;

/**
 * Test the php-redis adapter.
 *
 * @requires extension redis
 */
final class PhpRedisTest extends TestCase
{
    use AdapterTestTrait;
    use DumpTestTrait;
    use BucketTestTrait;
    use LockingTestTrait;


    public function createRdb(): Rdb
    {
        return Rdb::create([
            'prefix' => uniqid('rdb:') . ':',
            'adapter' => RdbConfig::TYPE_PHP_REDIS,
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
