<?php
require dirname(__DIR__) . '/vendor/autoload.php';

use karmabunny\rdb\Rdb;
use kbtests\SessionServer;


$adapter = $_GET['adapter'];

if ($adapter != 'none') {
    if (!isset(SessionServer::ADAPTERS[$adapter])) {
        http_response_code(400);
        header('Content-Type: application/json');
        echo json_encode([
            'adapter' => $adapter,
            'error' => 'Invalid adapter',
        ], JSON_PRETTY_PRINT);
        return false;
    }

    $config = SessionServer::ADAPTERS[$adapter];
    $config['prefix'] = 'test:';

    $rdb = Rdb::create($config);
    $rdb->registerSessionHandler();
}

session_start();

$key = $_GET['key'] ?? null;
$value = $_GET['value'] ?? null;

if (!$key) {
    header('HTTP/1.1 400 Bad Request');
    header('Content-Type: application/json');
    echo json_encode([
        'adapter' => $adapter,
        'error' => 'Missing key',
    ], JSON_PRETTY_PRINT);
    return false;
}

if ($value) {
    $_SESSION[$key] = $value;
}

$headers = [];
foreach ($_SERVER as $name => $value) {
    if (strpos($name, 'HTTP_') === 0) {
        $header = substr($name, 5);
        $header = strtolower(str_replace('_', '-', $header));
        $headers[$header] = $value;
    }
}

header('Content-Type: application/json');
echo json_encode([
    'adapter' => $adapter,
    'session_id' => session_id(),
    'headers' => $headers,
    'key' => $key,
    'value' => $_SESSION[$key] ?? null,
], JSON_PRETTY_PRINT);

return true;
