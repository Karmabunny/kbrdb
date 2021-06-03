<?php

namespace kbtests;

use karmabunny\rdb\Rdb;
use karmabunny\rdb\RdbConfig;

/**
 * Test the predis adapter.
 */
final class PredisTest extends AdapterTestCase
{
    public function setUp(): void
    {
        static $rdb;
        if (!$rdb) $rdb = Rdb::create([
            'prefix' => 'rdb:',
            'adapter' => RdbConfig::TYPE_PREDIS,
        ]);

        $this->rdb = $rdb;
        $rdb->del($rdb->keys('*'));
    }
}
