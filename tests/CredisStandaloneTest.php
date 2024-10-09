<?php

namespace kbtests;

use karmabunny\rdb\Rdb;
use karmabunny\rdb\RdbConfig;

/**
 * Test the credis in standalone (without php-redis).
 */
final class CredisStandaloneTest extends AdapterTestCase
{
    public function createRdb(): Rdb
    {
        return Rdb::create([
            'prefix' => uniqid('rdb:') . ':',
            'adapter' => RdbConfig::TYPE_CREDIS,
            'options' => [
                'standalone' => true,
            ]
        ]);
    }
}
