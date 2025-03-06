<?php

namespace kbtests;

use karmabunny\rdb\RdbConfig;
use karmabunny\visor\Server;


class SessionServer extends Server
{

    const ADAPTERS = [
        'predis' => [
            'adapter' => RdbConfig::TYPE_PREDIS,
        ],
        'predis-native' => [
            'adapter' => RdbConfig::TYPE_PREDIS,
            'options' => [
                'use_predis_session' => true,
            ],
        ],
        'phpredis' => [
            'adapter' => RdbConfig::TYPE_PHP_REDIS,
        ],
        'phpredis-native' => [
            'adapter' => RdbConfig::TYPE_PHP_REDIS,
            'options' => [
                'use_native_session' => true,
            ],
        ],
        'credis' => [
            'adapter' => RdbConfig::TYPE_CREDIS,
        ],
        'credis-standalone' => [
            'adapter' => RdbConfig::TYPE_CREDIS,
            'options' => [
                'standalone' => true,
            ],
        ],
    ];


    public function getTargetScript(): string
    {
        return __DIR__ . '/server.php';
    }
}
