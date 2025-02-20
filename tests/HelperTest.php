<?php

namespace kbtests;

use karmabunny\rdb\Rdb;
use karmabunny\rdb\RdbHelperTrait;
use PHPUnit\Framework\TestCase;

/**
 * Test the predis adapter.
 */
final class HelperTest extends TestCase
{

    public function dataMatch()
    {
        return [
            ['h?llo', ['hello', 'hallo', 'hxllo'], ['heello']],
            ['h*llo', ['hllo', 'heeeello', 'helllllo'], ['hlo', 'helloo']],
            ['h[ae]llo', ['hello', 'hallo'], ['hllo', 'hillo']],
            ['h[^e]llo', ['hallo', 'hzllo'], ['hllo', 'hello']],
            ['h[a-b]llo', ['hallo', 'hbllo'], ['hllo', 'hcllo']],
        ];
    }


    /**
     * @dataProvider dataMatch
     */
    public function testMatch($pattern, $good, $bad)
    {
        foreach ($good as $key) {
            $actual = Rdb::match($pattern, $key);
            $this->assertTrue($actual, "{$pattern} - {$key}");
        }

        foreach ($bad as $key) {
            $actual = Rdb::match($pattern, $key);
            $this->assertFalse($actual, "{$pattern} - {$key}");
        }
    }


    public function testFlatten()
    {
        $actual = Helper::flatten([
            [
                10 => 123,
                30 => 789,
                'overwrite' => 'nope',
            ],
            [
                'overwrite' => 'yas',
                20 => 456,
                [
                    'abc',
                    'def',
                ],
            ],
            [
                'ghi',
                'jkl',
            ],
        ]);

        $expected = [
            123,
            789,
            'nope',
            'yas',
            456,
            'abc',
            'def',
            'ghi',
            'jkl',
        ];

        $this->assertEquals($expected, $actual);
    }


    public function testFlattenIterable()
    {
        $it = (function() {
            yield 123;
            yield (function() {
                yield 'abc';
                yield 'def';
                yield (function() {
                    yield 'how';
                    yield (function() {
                        yield 'deep';
                        yield from (function() {
                            yield 'can';
                            yield 'you';
                            yield 'go';
                        })();
                    })();
                })();
                yield 'ghi';
            })();
            yield 456;
        });

        $actual = Helper::flatten($it());
        $expected = [
            123,
            'abc',
            'def',
            'how',
            'deep',
            'can',
            'you',
            'go',
            'ghi',
            456,
        ];

        $this->assertEquals($expected, $actual);

        // Test max depth.
        $actual = Helper::flatten($it(), 3);
        $expected = [
            123,
            'abc',
            'def',
            'how',
            'ghi',
            456,
        ];

        $this->assertEquals($expected, $actual);
    }


    public function testSetFlags()
    {

    }


    public function testRangeFlags()
    {

    }


    public function testFlattenKeys()
    {
        $actual = Helper::flattenKeys([
            'abc' => [
                'def' => 123,
                'ghi' => [ 456, 789 ],
            ],
        ]);

        $expected = [
            'abc.def' => 123,
            'abc.ghi.0' => 456,
            'abc.ghi.1' => 789,
        ];

        $this->assertEquals($expected, $actual);
    }


    public function testExplodeFlatKeys()
    {
        $actual = Helper::explodeFlatKeys([
            'abc.def' => 123,
            'abc.ghi.0' => 456,
            'abc.ghi.1' => 789,
        ]);

        $expected = [
            'abc' => [
                'def' => 123,
                'ghi' => [ 456, 789 ],
            ],
        ];

        $this->assertEquals($expected, $actual);
    }
}

class Helper
{
    use RdbHelperTrait {
        flatten as public;
        parseSetFlags as public;
        parseRangeFlags as public;
        flattenKeys as public;
        explodeFlatKeys as public;
    }
}
