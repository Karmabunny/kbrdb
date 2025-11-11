<?php

namespace kbtests;

use karmabunny\rdb\Rdb;

if (isset($argv[0]) && realpath($argv[0]) === __FILE__) {
    require __DIR__ . '/../vendor/autoload.php';

    [, $adapter, $prefix, $key, $wait] = $argv;

    usleep($wait * 1000000);
    $rdb = Rdb::create([ 'prefix' => $prefix, 'adapter' => $adapter ]);
    $ok = $rdb->del($key);
    return null;
}

class Release {
    static function release(string $adapter, string $prefix, string $key, float $wait) {
        exec('php ' . __FILE__ . " {$adapter} {$prefix} {$key} {$wait} > /dev/null 2>&1 &");
    }
}
