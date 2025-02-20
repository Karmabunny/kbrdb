

### v1.0

Fix phpstan issues.


### v1.1

Fix mGetObjects.
- Fix PX-ttl thing in predis adapter.
- Fix broken mGetObjects.
- Fix minor php-redis adapter things.
- Prevent empty arrays + args from making things blow up.
- Fix php compat for 7.1.


### v1.2

Return object sizes.


### v1.3

Added set remove
- sRem().


### v1.4

Locks


### v1.5

Credis adapter.


### v1.6

Added leaky bucket rate limiting utility.
- Support for php 7.0.


### v1.7

Added static rdb typings.
- Rename init -> getInstance for static compat class.


### v1.8

Added ttl for setJson, setObject.
- Fix mGetObjects with non-sequential/numeric keys.


### v1.9

Extended set flags, iterable keys for mScanObjects, lock extend, fix non-numeric mGetObjects.


### v1.10

Milliseconds for locks.


### v1.11

Subtle breaking change for lock_sleep.


### v1.12

Add incr/decr methods, optionally include null in object lists.
- Accept interfaces when asserting object types.


### v1.13

Added support for lists, lots of bug fixes, more tests.


### v1.14

Configurable timeouts, mget/mset family return keyed arrays, more untested methods.


### v1.15

Iterable keys, bump minimum php 7.1.


### v1.16

Consistent return null for invalid-types. Bunch of bug fixes everywhere.


### v1.17

Fix php-redis session path.


### v1.18

Support for sorted sets.


### v1.19

Add append/range methods for strings.
- Fix milliseconds for predis ttl.


### v1.20

Fix + ignore bad typings.
- Fix static rdb instance persistence.
- Fix json type signatures.


### v1.21

dump/restore and export/import helpers.


### v1.22

-m


### v1.23

Add select/move helpers.


### v1.24

Add sscan, new scan_size config.
- IncrBy and IncrByFloat


### v1.25

Add shorthand configuration for dump tools.


### v1.26

Add tests for flush.
- Fix excludes if include is not wildcard.
- Fix const in trait.


### v1.27

Support for hash types.


### v1.28

New object driver for alternate object serialisers.
