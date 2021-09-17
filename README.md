
# Rdb

Like Pdb, but for Redis. I guess.

Big emphasis on prefixing keys and object serialization.

Also because I don't want a hard dependency on either predis or php-redis. They both have their problems. ~~Or wouldn't it be wonderful if a 3rd option showed up /s~~. Also supports credis.


## Install

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


### Methods

- get
- set
- keys
- scan
- mGet
- mScan
- mSet
- mGetObjects
- mScanObject
- mSetObjects
- setJson
- getJson
- del
- exists
- sMembers
- sAdd

TODO: more


### Adapters

- php-redis (binary extension)
- predis (composer package)
- credis (composer package)


### Config

- `host` - server name + port
- `prefix` - key prefix
- `adapter` - 'predis' (default), 'php-redis', 'credis'
- `chunk_size` - max key size for scan methods
- `options` - adapter specific options

Notes:

- The port number is default 6379 unless specified in the `host` option.
- The protocol can be adjusted in the `host` option too: prefix `tcp://` or `udp://`.


### TODOs

- tests
- more methods
- Static version for Sprout compat
