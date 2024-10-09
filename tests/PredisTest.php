<?php

namespace kbtests;

use karmabunny\rdb\Rdb;
use karmabunny\rdb\RdbConfig;

/**
 * Test the predis adapter.
 */
final class PredisTest extends AdapterTestCase
{
    public function createRdb(): Rdb
    {
        return Rdb::create([
            'prefix' => uniqid('rdb:') . ':',
            'adapter' => RdbConfig::TYPE_PREDIS,
        ]);
    }
}
