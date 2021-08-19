<?php
require __DIR__ . '/../vendor/autoload.php';

use karmabunny\rdb\Rdb;

function main() {
    $rdb = Rdb::create([ 'prefix' => 'rdb:' ]);

    $bucket = $rdb->getBucket([
        'key' => 'bucket_test',
        'capacity' => 10,
        'drip_rate' => 1,
    ]);

    $ok = $bucket->drip();
    echo $ok ? 'ok': 'limit', "\n";

    $status = $bucket->getStatus();

    foreach ($status as $key => $value) {
        echo "{$key}: {$value}\n";
    }
}

while (true) {
    main();
    fgets(STDIN);
}
