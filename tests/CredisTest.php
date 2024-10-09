<?php

namespace kbtests;

use karmabunny\rdb\Rdb;
use karmabunny\rdb\RdbConfig;

/**
 * Test the credis adapter while wrapping php-redis.
 *
 * @requires extension redis
 */
final class CredisTest extends AdapterTestCase
{
    public function createRdb(): Rdb
    {
        return Rdb::create([
            'prefix' => uniqid('rdb:') . ':',
            'adapter' => RdbConfig::TYPE_CREDIS,
        ]);
    }
}
