<?php

namespace kbtests;

use karmabunny\rdb\Rdb;
use karmabunny\rdb\RdbConfig;

/**
 * Test the credis adapter while wrapping php-redis.
 *
 * @requires php-redis
 */
final class CredisTest extends AdapterTestCase
{
    public function setUp(): void
    {
        static $rdb;
        if (!$rdb) $rdb = Rdb::create([
            'prefix' => 'rdb:',
            'adapter' => RdbConfig::TYPE_CREDIS,
        ]);

        $this->rdb = $rdb;
        $rdb->del($rdb->keys('*'));
    }
}
