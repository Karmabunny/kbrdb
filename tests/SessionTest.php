<?php

namespace kbtests;

use karmabunny\rdb\Rdb;
use PHPUnit\Framework\TestCase;

/**
 * Test session handlers.
 */
final class SessionTest extends TestCase
{

    /** @var Rdb[] */
    public $rdb = [];

    /** @var SessionServer */
    public $server = null;


    public function setUp(): void
    {
        $this->server ??= new SessionServer([
            'path' => dirname(__DIR__),
        ]);
        $this->server->start();
    }


    public function tearDown(): void
    {
        $this->server->stop();
    }


    public function dataAdapters()
    {
        $adapters = array_keys(SessionServer::ADAPTERS);
        $adapters = array_combine($adapters, $adapters);
        $adapters = array_map(fn($adapter) => [$adapter], $adapters);
        $adapters = array_merge(['none' => ['none']], $adapters);
        return $adapters;
    }


    /**
     * @dataProvider dataAdapters
     */
    public function testSession(string $adapter)
    {
        // No cookie.
        $value1 = bin2hex(random_bytes(10));

        $res = $this->request([
            'adapter' => $adapter,
            'key' => 'test',
            'value' => $value1,
        ]);

        $this->assertEquals(200, $res['status'], 'Status should be 200 OK');
        $this->assertArrayHasKey('set-cookie', $res['headers']);

        $cookie1 = $res['headers']['set-cookie'];

        // Another, should get a different session.
        $value2 = bin2hex(random_bytes(10));

        $res = $this->request([
            'adapter' => $adapter,
            'key' => 'test',
            'value' => $value2,
        ]);

        $this->assertEquals(200, $res['status'], 'Status should be 200 OK');
        $this->assertArrayHasKey('set-cookie', $res['headers']);

        $cookie2 = $res['headers']['set-cookie'];
        $this->assertNotEquals($cookie1, $cookie2, 'Cookie should be different');

        // Get a value from session1.
        $res = $this->request([
            'adapter' => $adapter,
            'key' => 'test',
        ], ['Cookie' => $cookie1]);

        $this->assertEquals(200, $res['status'], 'Status should be 200 OK');
        $this->assertEquals($value1, $res['body']['value'], json_encode($res, JSON_PRETTY_PRINT));

        // Get a value from session2.
        $res = $this->request([
            'adapter' => $adapter,
            'key' => 'test',
        ], ['Cookie' => $cookie2]);

        $this->assertEquals(200, $res['status'], 'Status should be 200 OK');
        $this->assertEquals($value2, $res['body']['value'], json_encode($res, JSON_PRETTY_PRINT));
    }



    public function request(array $params = [], array $headers = [])
    {
        $url = $this->server->getHostUrl() . '?' . http_build_query($params);

        $request_headers = '';
        foreach ($headers as $name => $value) {
            $request_headers .= "{$name}: {$value}\r\n";
        }

        $context = stream_context_create([
            'http' => [
                'method' => 'GET',
                'ignore_errors' => true,
                'header' => $request_headers,
            ],
        ]);

        $body = file_get_contents($url, false, $context);
        $body = json_decode($body, true) ?: [];

        $status_code = 0;
        $response_headers = [];

        foreach ($http_response_header ?? [] as $header) {
            $matches = [];

            if (!$status_code and preg_match('/ ([0-9]+) /', $header, $matches)) {
                $status_code = $matches[1];
            }

            if (strpos($header, ':') === false) {
                continue;
            }

            [$name, $value] = explode(':', $header, 2);
            $name = strtolower(trim($name));
            $value = trim($value);

            $response_headers[$name] = $value;
        }

        return [
            'body' => $body,
            'status' => $status_code,
            'headers' => $response_headers,
        ];
    }
}
