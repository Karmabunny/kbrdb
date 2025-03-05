<?php

namespace kbtests;

use karmabunny\rdb\Rdb;
use PHPUnit\Framework\TestCase;


/**
 * Test the leaky bucket.
 *
 * TODO Should probably test alternate drip-rates.
 *
 * @mixin TestCase
 * @property Rdb $rdb
 */
trait BucketTestTrait
{

    /**
     * Typical drip stuff.
     */
    public function testBasic()
    {
        $bucket = $this->rdb->getBucket([
            'key' => 'test',
            'capacity' => 5,
            'drip_rate' => 1,
        ]);

        // Drip once.
        $this->assertTrue($bucket->drip());
        sleep(1);

        // Drip a bunch more.
        $this->assertTrue($bucket->drip());
        $this->assertTrue($bucket->drip());
        $this->assertTrue($bucket->drip());
        $this->assertTrue($bucket->drip());

        // Oh no we're full!
        $this->assertFalse($bucket->drip());

        $status = $bucket->getStatus();

        // Stats are ok.
        $this->assertEquals('5/5', $status['level']);
        $this->assertEquals(1.0, $status['drip_rate']);
        $this->assertGreaterThan(0, $status['wait']);
        $this->assertLessThan(5001, $status['wait']);

        // Definitely full, yep.
        $this->assertTrue($bucket->isFull());
        $this->assertEquals(5, $bucket->getLevel());

        // Reload from storage.
        $bucket->refresh();

        $this->assertTrue($bucket->isFull());
        $this->assertEquals(5, $bucket->getLevel());

        // Wait for that very first drip to leak out.
        sleep(4);
        $bucket->refresh();

        $this->assertFalse($bucket->isFull());
        $this->assertEquals(4, $bucket->getLevel());

        // 2 drips don't fit, but 1 does.
        $this->assertFalse($bucket->drip(2));
        $this->assertTrue($bucket->drip(1));
    }


    /**
     * One bucket doesn't affect the other.
     */
    public function testMultiple()
    {
        $bucket1 = $this->rdb->getBucket([
            'key' => 'test1',
            'capacity' => 5,
            'drip_rate' => 1,
        ]);

        $bucket2 = $this->rdb->getBucket([
            'key' => 'test2',
            'capacity' => 5,
            'drip_rate' => 1,
        ]);

        $bucket1->drip();
        $bucket1->drip();
        $bucket2->drip();

        $this->assertEquals(2, $bucket1->getLevel());
        $this->assertEquals(1, $bucket2->getLevel());

        $bucket1->refresh();
        $bucket2->refresh();

        $this->assertEquals(2, $bucket1->getLevel());
        $this->assertEquals(1, $bucket2->getLevel());
    }


    /**
     * Automatically determine costs for a type of drip.
     */
    public function testCosts()
    {
        $bucket = $this->rdb->getBucket([
            'key' => 'test',
            'capacity' => 10,
            'drip_rate' => 1,
            'costs' => [
                'get' => 1,
                'post' => 5,
            ],
        ]);

        $bucket->drip('get');
        $bucket->drip('get');
        $this->assertEquals(2, $bucket->getLevel());

        $this->assertTrue($bucket->drip('post'));
        $this->assertTrue($bucket->drip('get'));
        $this->assertEquals(8, $bucket->getLevel());

        // Reject, level stays the same.
        $this->assertFalse($bucket->drip('post'));
        $this->assertEquals(8, $bucket->getLevel());

        // This is ok.
        $this->assertTrue($bucket->drip('get'));
        $this->assertEquals(9, $bucket->getLevel());
    }

}
