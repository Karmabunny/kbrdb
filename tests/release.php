<?php

use karmabunny\rdb\Rdb;

if (realpath(@$argv[0]) === __FILE__) {
    require __DIR__ . '/../vendor/autoload.php';

    usleep($argv[2] * 1000000);
    $rdb = Rdb::create([ 'prefix' => 'rdb:' ]);
    $ok = $rdb->del($argv[1]);
    return null;
}

function release(string $key, float $wait) {
    exec('php ' . __FILE__ . " {$key} {$wait} > /dev/null 2>&1 &");
}
