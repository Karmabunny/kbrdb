<?php

namespace kbtests;

use karmabunny\rdb\Rdb;
use karmabunny\rdb\RdbLock;
use PHPUnit\Framework\TestCase;
use kbtests\Release;

/**
 * Test the locking mechanism.
 *
 * @mixin TestCase
 * @property Rdb $rdb
 */
trait LockingTestTrait
{

    public function testLock()
    {
        $time = microtime(true);
        $lock1 = $this->rdb->lock('lock:1', 1000);

        // Matching key + token.
        $this->assertEquals($lock1->key, 'lock:1');
        $this->assertEquals($lock1->token, $this->rdb->get('lock:1'));

        // No existing lock - no waiting, got a lock.
        $this->assertLessThan(0.1, microtime(true) - $time);
        $this->assertInstanceOf(RdbLock::class, $lock1);

        // Existing lock, no wait, no lock.
        $time = microtime(true);
        $lock2 = $this->rdb->lock('lock:1', 0);

        $this->assertNull($lock2);
        $this->assertLessThan(0.1, microtime(true) - $time);
    }


    public function testRelease()
    {
        $lock1 = $this->rdb->lock('lock:1', 0);
        $this->assertNotNull($lock1);

        // Release after 1/2 second off-thread.
        Release::release($this->rdb->config->adapter, $this->rdb->config->prefix, 'lock:1', 0.5);

        // Existing lock, waits 0.5, gets a lock.
        $time = microtime(true);
        $lock2 = $this->rdb->lock('lock:1', 1);

        $this->assertEqualsWithDelta(0.5, microtime(true) - $time, 0.1);
        $this->assertInstanceOf(RdbLock::class, $lock2);

        $this->assertNotEquals($lock1->token, $lock2->token);
    }


    public function testTimeout()
    {
        $lock1 = $this->rdb->lock('lock:1', 0, 0.5);
        $this->assertNotNull($lock1);

        usleep(0.25 * 1000000);
        $this->assertTrue((bool) $this->rdb->exists('lock:1'));

        usleep(0.75 * 1000000);
        $this->assertFalse((bool) $this->rdb->exists('lock:1'));
    }
}
