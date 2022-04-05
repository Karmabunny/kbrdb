<?php

namespace kbtests;

use karmabunny\rdb\Rdb;
use karmabunny\rdb\RdbConfig;

/**
 * Test the credis in standalone (without php-redis).
 */
final class CredisStandaloneTest extends AdapterTestCase
{
    public function setUp(): void
    {
        static $rdb;
        if (!$rdb) $rdb = Rdb::create([
            'prefix' => uniqid('rdb:') . ':',
            'adapter' => RdbConfig::TYPE_CREDIS,
            'options' => [
                'standalone' => true,
            ]
        ]);

        $this->rdb = $rdb;
        $rdb->del($rdb->keys('*'));
    }
}
