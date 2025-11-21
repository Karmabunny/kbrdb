<?php

namespace karmabunny\rdb;

use karmabunny\interfaces\MutexInterface;

/**
 *
 * @package karmabunny\rdb
 */
class RdbMultiMutex implements MutexInterface
{

    const LUA_RELEASE = <<<LUA
local values = redis.call("MGET", unpack(KEYS))
local count = 0
for index, value in ipairs(values) do
    if value == ARGV[index] then
        count = count + redis.call("DEL", KEYS[index])
    end
end
return count
LUA;

const LUA_ACQUIRE = <<<LUA
if redis.call("EXISTS", unpack(KEYS)) > 0 then
    return false
end
for index, key in ipairs(KEYS) do
    redis.call("SET", key, ARGV[index], "NX", "PX", ARGV[#ARGV])
end
return redis.call("MGET", unpack(KEYS))
LUA;

    /** @var Rdb */
    public $rdb;

    /** @var string[] */
    public $names;

    /** @var string */
    public $prefix = 'mutex:';

    /** @var bool */
    public $autoRelease = true;

    /** @var int in seconds, 0 for infinite */
    public $autoExpire = 60;

    /** @var string[] */
    protected $values = [];

    /**
     * @param Rdb $rdb
     * @param string[] $names
     */
    public function __construct(Rdb $rdb, array $names)
    {
        $this->rdb = $rdb;
        $this->names = $names;

        register_shutdown_function([$this, '__destruct']);
    }


    /** @inheritdoc */
    public function __destruct()
    {
        if ($this->autoRelease) {
            $this->release();
        }
    }


    /**
     * Join with an existing mutex.
     *
     * @return bool if successful and locked, otherwise unlocked.
     */
    public function resume(): bool
    {
        $keys = $this->getKeys();
        $this->values = $this->rdb->mGet($keys);
        $this->values = array_filter($this->values);
        return !empty($this->values);
    }


    /** @inheritdoc */
    public function acquire(float $timeout = 0): bool
    {
        if ($timeout <= 0) {
            return $this->tryAcquire();
        }
        else {
            $start = microtime(true);
            $tick = (int) ($this->rdb->config->lock_sleep * 1000);

            for (;;) {
                $ok = $this->tryAcquire();

                if ($ok) {
                    return $ok;
                }

                // Preventing infinite loops with a timeout.
                if (microtime(true) - $start >= $timeout) {
                    break;
                }

                usleep($tick);
            }

            return false;
        }
    }


    /** @inheritdoc */
    public function release(): bool
    {
        $keys = $this->getKeys();

        if (empty($this->values)) {
            return false;
        }

        $count = $this->rdb->eval(self::LUA_RELEASE, $keys, $this->values);

        if (!is_numeric($count)) {
            return false;
        }

        return $count != 0;
    }


    /**
     * Try to acquire the mutex.
     *
     * @return bool
     */
    protected function tryAcquire(): bool
    {
        $keys = $this->getKeys();
        $expire = $this->autoExpire * 1000;

        $value = $this->generateValue();
        $args = array_fill(0, count($keys), $value);
        $args[] = $expire;

        $values = $this->rdb->eval(self::LUA_ACQUIRE, $keys, $args);
        $this->values = (array) ($values ?: []);

        return !empty($this->values);
    }


    /**
     * Get the full key for this mutex.
     *
     * @return string[]
     */
    protected function getKeys(): array
    {
        $keys = $this->rdb->prefix($this->prefix, $this->names);
        return iterator_to_array($keys, false);
    }


    /**
     * Generate a value for the mutex.
     *
     * @return string
     */
    protected function generateValue(): string
    {
        return base64_encode(random_bytes(24));
    }
}
