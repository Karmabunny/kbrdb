
# Rdb

Like Pdb, but for Redis. I guess.

Big emphasis on prefixing keys and object serialization.

Also because I don't want a hard dependency on either predis or php-redis. They both have their problems. ~~Or wouldn't it be wonderful if a 3rd option showed up /s~~. Also supports credis.


### Install

Install with composer from our private package repo.

Add the repository:

```json
"repositories": [
    {
        "type": "composer",
        "url": "https://packages.bunnysites.com"
    }
]
```

Add as a dependency:

```sh
composer require karmabunny/rdb
```

### Adapters

- php-redis (binary extension)
- predis (composer package)
- credis (composer package)


### Config

- `host` - server name + port
- `prefix` - key prefix
- `adapter` - 'predis' (default), 'php-redis', 'credis'
- `chunk_size` - max key size for scan methods (default: 50)
- `lock_sleep` - tick size for locking, in seconds (default: 0.005)
- `options` - adapter specific options

Notes:

- The port number is default 6379 unless specified in the `host` option.
- The protocol can be adjusted in the `host` option too: prefix `tcp://` or `udp://`.

```php
return [
    'host' => 'localhost',
    'prefix' => 'sitecode:',

    // Defaults
    'adapter' => 'predis',
    'chunk_size' => 50,
    'lock_sleep' => 0.005,
    'options' => [],
];
```


### Usage

Basic usage with a TTL. Great for caching.

```php
use karmabunny\rdb\Rdb;

$config = require 'config.php';
$rdb = new Rdb($config);

// Store 'blah' for 100 ms
$rdb->set('key', 'blah', 100);

$rdb->get('key');
// => blah

usleep(150);

$rdb->get('key');
// => NULL
```

Object extensions will serialize in the PHP format. These have builtin assertions so things are always the correct shape.

```php
$model = new MyModel('etc');
$rdb->setObject('objects:key', $model);

$rdb->getObject('objects:key', MyModel::class);
// => MyModel( etc )

$rdb->getObject('objects:key', OtherModel::class);
// => NULL
```

Locking provides a mechanism to restrict atomic access to a resource.

```php
// Wait for a lock for up to 10 seconds.
$lock = $rdb->lock('locks:key', 10);

if ($lock === null) {
    echo "Too much contention\n";
}
else {
    // Do atomic things.
    $lock->release();
}
```

[Leaky bucket](https://en.wikipedia.org/wiki/Leaky_bucket) is a rate-limiting algorithm. It's cute, easy to understand, and not too complex.

```php
// A bucket with 60 drips per minute.
$bucket = $rdb->getBucket([
    'key' => 'buckets:key',

    // Defaults.
    'capacity' => 60,
    'drip_rate' => 1,

    // Optional.
    'prefix' => 'drips:',
    'costs' => [
        'GET' => 1,
        'POST' => 10,
    ],
]);

// One drip.
$full = $bucket->drip();

if ($full) {
    echo "We're full, please wait {$bucket->getWait()} ms\n";
}
else {
    // Do things.
}

// Big drip.
$bucket->drip(20);

// Named drip (10 drips).
$bucket->drip('POST');

// Write out the status to the headers for easy debugging.
$bucket->writeHeaders();
```


### Methods

#### Core Methods

- get
- set
- keys
- scan
- mGet
- mSet
- del
- exists
- sMembers
- sAdd

TODO: more

#### Extended Methods

- mScan
- getObject
- setObject
- mGetObjects
- mScanObjects
- mSetObjects
- setJson
- getJson

#### Builtin Utilities

- 'Leaky bucket' rate limiting
- Locking


### TODOs

- more tests
- more methods
