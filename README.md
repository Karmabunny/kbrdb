
# Rdb

Like Pdb, but for Redis. I guess.

This wraps existing Redis clients and normalises the API (between server versions and client implementations). It also introduces a few additional helpers:

- Object serialization
- Leaky bucket rate limiting
- Locking
- Export/import
- Session handler

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

| Key           | Default         | Description                             |
|---------------|-----------------|-----------------------------------------|
| host          | 127.0.0.1       | Server name + port                      |
| prefix        | ''              | Key prefix                              |
| adapter       | 'predis'        | Adapter type: predis, php-redis, credis |
| object_driver | PhpObjectDriver | Object driver class                     |
| timeout       | 5               | Connection timeout, in seconds          |
| lock_sleep    | 5               | Tick size for locking, in milliseconds  |
| chunk_size    | 50              | Max key size for mscan methods          |
| scan_size     | 1000            | Count hint for scan methods             |
| scan_keys     | false           | Replace keys() with scan()              |
| options       | []              | Adapter specific options                |

Notes:

- The port number is default 6379 unless specified in the `host` option.
- The protocol can be adjusted in the `host` option too: prefix `tcp://` or `udp://`.

```php
return [
    'host' => 'localhost',
    'prefix' => 'sitecode:',

    // Defaults
    'adapter' => 'predis',
    'object_driver' => JsonObjectDriver::class,
    'timeout' => 5,
    'lock_sleep' => 5,
    'chunk_size' => 50,
    'scan_size' => 1000,
    'scan_keys' => false,
    'options' => [],
];
```

#### Adapter options

__Predis__

The predis adapter accepts any options supported by the Predis client.

| Option             | Description                        |
|--------------------|------------------------------------|
| use_predis_session | Use the predis session handler     |
| exceptions         | Enable exceptions on errors        |
| connections        | Connection settings                |
| cluster            | Cluster configuration              |
| replication        | Replication configuration          |

__PhpRedis__


| Option             | Description                        |
|--------------------|------------------------------------|
| use_native_session | Use the native session handler     |
| retry_interval     | Time between reconnection attempts |
| read_timeout       | Socket read timeout                |

__Credis__


| Option             | Description                        |
|--------------------|------------------------------------|
| use_native_session | only if `standalone = false`       |
| standalone         | Force standalone mode vs phpredis  |



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

- keys
- scan
- del
- exists
- ttl
- expire
- expireat
- type
- rename
- move
- select
- dump/restore

scalar:
- get
- set
- mGet
- mSet
- append
- substr
- getRange
- incrBy
- decrBy
- incrByFloat
- decrByFloat

sets:
- sMembers
- sAdd
- sRem
- sCard
- sIsMember
- sMove
- sScan
- TODO sRandMember
- TODO sDiff (+ store)
- TODO sInter (+ store)
- TODO sUnion (+ store)

lists:
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

sorted sets:
- zAdd
- zIncrBy
- zRange
- zRem
- zCard
- zCount
- zScore
- zRank
- zRevRank

hashes:
- hSet
- hGet
- hExists
- hDel
- hmSet
- hmGet
- hGetAll
- hIncrBy/hIncrByFloat
- hKeys
- hVals
- hLen
- hScan
- hStrLen


#### Extended/Wrapper Methods

- mScan
- getObject
- setObject
- mGetObjects
- mScanObjects
- mSetObjects
- setJson
- getJson
- setHash/getHash (nested arrays in hash)
- pack/unpack (MessagePack format)
- incr/decr
- hIncr/hDecr
- export/import


#### Builtin Utilities

- 'Leaky bucket' rate limiting
- Locking

