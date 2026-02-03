<?php

use UniForceMusic\PHPDuckDBCLI\Integrations\Sentience\DuckDBDatabase;

include 'vendor/autoload.php';

$db = DuckDBDatabase::memory(debug: function (string $query, float $startTime, ?string $error) {
    echo '-------------------------------------------' . PHP_EOL;
    echo "Query: {$query}" . PHP_EOL;
    echo "Time: " . (microtime(true) - $startTime) * 1000;
    echo '-------------------------------------------' . PHP_EOL;
});

$db->createTable('test')
    ->ifNotExists()
    ->identity('id')
    ->string('name')
    ->primaryKeys(['id'])
    ->execute();

$db->insert('test')
    ->values([
        'name' => 'test'
    ])
    ->execute();

$db->insert('test')
    ->values([
        'name' => 'test2'
    ])
    ->execute();

$db->insert('test')
    ->values([
        'name' => 'test3'
    ])
    ->execute();

print_r($db->select('test')->execute()->fetchAssocs());
