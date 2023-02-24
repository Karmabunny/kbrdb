<?php

namespace kbtests;

use karmabunny\rdb\RdbHelperTrait;
use PHPUnit\Framework\TestCase;

/**
 * Test the predis adapter.
 */
final class HelperTest extends TestCase
{

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
}

class Helper
{
    use RdbHelperTrait {
        flatten as public;
        parseSetFlags as public;
        parseRangeFlags as public;
    }
}
