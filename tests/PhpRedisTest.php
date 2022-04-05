<?php

namespace kbtests;

use karmabunny\rdb\Rdb;
use karmabunny\rdb\RdbConfig;

/**
 * Test the php-redis adapter.
 *
 * @requires extension redis
 */
final class PhpRedisTest extends AdapterTestCase
{
    public function setUp(): void
    {
        static $rdb;
        if (!$rdb) $rdb = Rdb::create([
            'prefix' => uniqid('rdb:') . ':',
            'adapter' => RdbConfig::TYPE_PHP_REDIS,
        ]);

        $this->rdb = $rdb;
        $rdb->del($rdb->keys('*'));
    }
}
