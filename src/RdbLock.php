<?php
/**
 * @link      https://github.com/Karmabunny
 * @copyright Copyright (c) 2021 Karmabunny
 */

namespace karmabunny\rdb;


/**
 * A redis lock is just a key with a token.
 *
 * Essentially, this provides a mechanism to wait until a key is deleted.
 *
 * Most of the locking logic is contained in `Rdb::lock()`.
 *
 * Inspired by:
 * https://github.com/cheprasov/php-redis-lock
 *
 * @package karmabunny\rdb
 */
class RdbLock
{

    /** @var Rdb */
    public $rdb;

    /** @var string */
    public $key;

    /** @var string */
    public $token;


    /**
     * A new lock!
     *
     * Don't use this, use {@see Rdb::lock}.
     *
     * @param Rdb $rdb
     * @param string $key
     * @param float $timeout seconds
     */
    public function __construct(Rdb $rdb, string $key, float $timeout = 60)
    {
        $this->rdb = $rdb;
        $this->key = $key;
        $this->token = self::createToken();

        $timeout *= 1000;
        $this->rdb->set($key, $this->token, (int) $timeout);
    }


    /** @inheritdoc */
    public function __destruct()
    {
        $this->release();
    }


    /**
     * Release this lock.
     *
     * Call this when you're done and happy.
     *
     * @return void
     */
    public function release()
    {
        if ($this->isLocked()) {
            $this->rdb->del($this->key);
        }
    }


    /**
     * Is the lock acquired?
     *
     * This should always be 'true' after obtaining a fresh lock.
     *
     * Reasons why this could be false:
     * - you released the lock
     * - someone deleted the key, flushdb, etc
     * - the TTL expired
     *
     * @return bool
     */
    public function isLocked(): bool
    {
        return $this->rdb->get($this->key) == $this->token;
    }


    /**
     * Something unique so we can figure out the key is truly locked
     * by _this_ lock.
     *
     * @return string
     */
    protected static function createToken(): string
    {
        return getmypid() . (int) microtime(true) . mt_rand(1, 9999);
    }
}
