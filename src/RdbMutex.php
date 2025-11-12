<?php

namespace karmabunny\rdb;

use karmabunny\interfaces\MutexInterface;

/**
 *
 * @package karmabunny\rdb
 */
class RdbMutex implements MutexInterface
{

    const LUA_RELEASE = <<<LUA
if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("DEL", KEYS[1])
else
    return 0
end
LUA;

    /** @var Rdb */
    public $rdb;

    /** @var string */
    public $name;

    /** @var string */
    public $prefix = 'mutex:';

    /** @var bool */
    public $autoRelease = true;

    /** @var int in seconds, 0 for infinite */
    public $autoExpire = 60;

    /** @var string|null */
    protected $value = null;

    /**
     * @param Rdb $rdb
     * @param string $name
     */
    public function __construct(Rdb $rdb, string $name)
    {
        $this->rdb = $rdb;
        $this->name = $name;

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
        $key = $this->getKey();
        $this->value = $this->rdb->get($key);
        return $this->value !== null;
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
        $key = $this->getKey();

        if ($this->value === null) {
            return false;
        }

        $count = $this->rdb->eval(self::LUA_RELEASE, [$key], [$this->value]);

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
        $key = $this->getKey();
        $expire = $this->autoExpire * 1000;

        $value = $this->generateValue();
        $ok = $this->rdb->set($key, $value, $expire, ['replace' => 'NX']);

        if ($ok) {
            $this->value = $value;
            return true;
        }

        return false;
    }


    /**
     * Get the full key for this mutex.
     *
     * @return string
     */
    protected function getKey(): string
    {
        return $this->prefix . $this->name;
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
