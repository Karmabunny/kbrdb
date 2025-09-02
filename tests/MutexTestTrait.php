<?php

namespace kbtests;

use karmabunny\interfaces\MutexInterface;
use karmabunny\rdb\Rdb;
use karmabunny\rdb\RdbMutex;
use PHPUnit\Framework\TestCase;

/**
 * Test the locking mechanism.
 *
 * @mixin TestCase
 * @property Rdb $rdb
 */
trait MutexTestTrait
{

    protected function createMutex(string $name)
    {
        return new RdbMutex($this->rdb, $name);
    }


    public function testMutexLock()
    {
        $time = microtime(true);
        $lock1 = $this->createMutex('test:1');
        $this->assertTrue($lock1->acquire(0));

        $key1 = $lock1->prefix . $lock1->name;
        $value1 = $lock1->rdb->get($key1);
        $this->assertNotNull($value1);

        $ttl = $lock1->rdb->ttl($key1);
        $this->assertGreaterThan(($lock1->autoExpire - 1) * 1000, $ttl);

        // No existing lock - no waiting, got a lock.
        $this->assertLessThan(0.01, microtime(true) - $time);
        $this->assertInstanceOf(MutexInterface::class, $lock1);

        // New lock, no collision, no wait.
        $lock2 = $this->createMutex('test:2');
        $this->assertTrue($lock2->acquire(0));

        // Existing lock, collision, immediate failure.
        $time = microtime(true);
        $lock3 = $this->createMutex('test:1');
        $this->assertFalse($lock3->acquire(0));
        $this->assertLessThan(0.01, microtime(true) - $time);

        // Existing lock, collision, failure after timeout.
        $time = microtime(true);
        $lock3 = $this->createMutex('test:1');
        $this->assertFalse($lock3->acquire(0.5));
        $this->assertGreaterThan(0.5, microtime(true) - $time);

        $this->assertTrue($lock1->release());

        // Try again - with success.
        $lock3 = $this->createMutex('test:1');
        $this->assertTrue($lock3->acquire(0));
        $this->assertEquals($lock1->name, $lock3->name);

        // Check values.
        $value3 = $this->rdb->get($key1);

        $this->assertNotNull($value3);
        $this->assertNotEquals($value1, $value3);
    }


    public function testMutexAutoExpire()
    {
        $lock1 = $this->createMutex('test:5');
        $lock1->autoExpire = 1;
        $this->assertTrue($lock1->acquire(0));

        usleep(1.1 * 1000000);

        $lock2 = $this->createMutex('test:5');
        $this->assertTrue($lock2->acquire(0));
    }

}
