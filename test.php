<?php

use UniForceMusic\PHPDuckDBCLI\Integrations\Sentience\DuckDBDatabase;

include 'vendor/autoload.php';

$debug = function (string $query, float $start, ?string $error = null): void {
    $end = microtime(true);

    $lines = [
        sprintf('Timestamp : %s', (new DateTime())->format('Y-m-d H:i:s.u')),
        sprintf('Query     : %s', $query),
        sprintf('Time      : %.2f ms', ($end - $start) * 1000)
    ];

    if ($error) {
        $lines[] = sprintf('Error     : %s', $error);
    }

    echo str_repeat('=', 120) . PHP_EOL;
    echo implode(PHP_EOL, $lines) . PHP_EOL;
    echo str_repeat('=', 120) . PHP_EOL;
};

$database = DuckDBDatabase::memory(debug: $debug);

$database->createTable('test')
    ->ifNotExists()
    ->identity('id')
    ->string('name')
    ->dateTime('created_at')
    ->primaryKeys(['id'])
    ->uniqueConstraint(['name'], 'test_uniq')
    ->execute();

$database->insert('test')
    ->values([
        'name' => 'test',
        'created_at' => new DateTime('2026-01-01 15:16:17.372869')
    ])
    ->onConflictUpdate(['name'])
    ->execute();

$database->insert('test')
    ->values([
        'name' => 'test2',
        'created_at' => '2026-01-01'
    ])
    ->onConflictIgnore(['name'])
    ->lastInsertId('id')
    ->execute();

$result = $database->select('test')
    ->execute();

print_r($result->fetchAssocs());
