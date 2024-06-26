
# Rdb

Like Pdb, but for Redis. I guess.

Big emphasis on prefixing keys and object serialization.

Also because I don't want a hard dependency on either predis or php-redis. They both have their problems (vague magical commands API, binary extension, etc). ~~Or wouldn't it be wonderful if a 3rd option showed up /s~~. Also supports credis.


### Install

Add as a dependency:

```sh
composer require karmabunny/rdb
```


### Adapters

- php-redis (binary extension)
- predis (composer package)
- credis (composer package)

This library doesn't implement a redis client itself, only wraps existing clients. After much effort trying to normalise the responses from all these client, it might  seem like a good idea to just write our own that isn't so inconsistent.

But consider that we only _know_ how inconsistent these libraries are because we've spent so much time trying to make them all behave the same. For example, client 'A' might do 'B' well but 'C' badly. Then client 'D' does 'B' badly but 'C' really well.

So as I sit here and scoff at their feeble attempts, I am reminded of a few things:

1. I've already introduced so many of my own bugs during this journey.
2. Unit testing is a gift from heaven.
3. Normalising these inconsistencies has improved our own consistency, something probably not as achievable when writing a new client from scratch.
4. also this: https://xkcd.com/927


### Version support

This wrapper doesn't try to polyfill any missing features. It targets Redis server v3.0, as that's the common support among all the adapters.

This library wouldn't ever try to _hide_ features behind target versions, but perhaps it could help smooth out any differences. Lua scripting could polyfill a lot of things tbh.

For example, `BRPOPLPUSH` is deprecated in v6.2 and might be removed in the distant future. In this case, the library would be able to dynamically replace (based on the server version) this with `BLMOVE`.


### Plans for v2

There is a preference for the millisecond version of a command, particularly TTL parameters. This is clearly misleading and already wildly inconsistent. Ideally this changes so that a 'float' is converted to the millisecond version and integer remains unchanged. Thus the input is always 'seconds'.

Type errors are currently (hopefully) always a `null` return. This can quite confusing at times, or helpful in others. Version 2 will likely permit both, defaulting to emitting exceptions.


### Config

- `host` - server name + port
- `prefix` - key prefix
- `adapter` - 'predis' (default), 'php-redis', 'credis'
- `timeout` - connection timeout, in seconds (default: 5)
- `lock_sleep` - tick size for locking, in milliseconds (default: 5)
- `chunk_size` - max key size for mscan methods (default: 50)
- `scan_size` - count hint for scan methods (default: 1000)
- `scan_keys` - replace keys() with scan() (default: false)
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
    'lock_sleep' => 5,
    'chunk_size' => 50,
    'scan_size' => 1000,
    'scan_keys' => false,
    'options' => [],
];
```


### Usage

Basic usage with a TTL. Great for caching.

```php
use karmabunny\rdb\Rdb;

$config = require 'config.php';
$rdb = Rdb::create($config);

// Store 'blah' for 100 ms
$rdb->set('key', 'blah', 100);

$rdb->get('key');
// => blah

usleep(150 * 1000);

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
$lock = $rdb->lock('locks:key', 10 * 1000);

if ($lock === null) {
    echo "Busy - too much contention\n";
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
    'key' => 'key',

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


### Contributing

Submit a PR if you like. But before you do, please do the following:

1. Run `composer analyse` and fix any complaints there
2. Run `composer compat` and fix those too
3. Write some tests and run `composer tests`
4. Document the methods here


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
- lLen
- lRange
- lTrim
- lSet
- lRem
- lIndex
- lPush
- lPop
- blPop
- rPush
- rPop
- brPop
- brPoplPush
- zAdd
- zIncrBy
- zRange
- zRem
- zCard
- zCount
- zScore
- zRank
- zRevRank


TODO: more

sets:
- sScan
- sRandMember
- sDiff (+ store)
- sInter (+ store)
- sUnion (+ store)


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
