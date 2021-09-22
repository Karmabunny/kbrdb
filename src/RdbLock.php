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
 * If the thread dies or you forget to call `release()` the object `__destroy()`
 * will release the lock. If not, the timeout eventually kicks in.
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
     * @param Rdb $rdb
     * @param string $key
     * @param string $token
     */
    protected function __construct(Rdb $rdb, string $key, string $token)
    {
        $this->rdb = $rdb;
        $this->key = $key;
        $this->token = $token;
    }


    /** @inheritdoc */
    public function __destruct()
    {
        $this->release();
    }


    /**
     * A new lock!
     *
     * @param Rdb $rdb
     * @param string $key
     * @param int $wait milliseconds
     * @param int $timeout milliseconds
     */
    public static function acquire(Rdb $rdb, string $key, int $wait = 0, int $timeout = 60000)
    {
        $token = self::createToken();

        $wait = microtime(true) + ($wait / 1000);
        $tick = (int) ($rdb->config->lock_sleep * 1000000);

        // Begin a wait loop until the lock is free.
        while (true) {
            $ok = $rdb->set($key, $token, $timeout, [
                'replace' => 'NX',
            ]);

            // Released!
            if ($ok) break;

            // Preventing infinite loops with a timeout.
            if ($wait < microtime(true)) break;

            usleep($tick);
        }

        // Lock still exists, no dice.
        if (!$ok) return null;

        return new RdbLock($rdb, $key, $token);
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
        // TODO It'd be cool if this were a little more atomic. Cue lua support.

        // It's important to check ownership here. When the object is destroyed
        // it always calls release() and could step on some toes.
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
        return base64_encode(random_bytes(24));
    }
}
