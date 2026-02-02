<?php

use UniForceMusic\PHPDuckDBCLI\Integrations\Sentience\DuckDBDatabase;

include 'vendor/autoload.php';

$db = DuckDBDatabase::fromMemory();

$db->createTable('test')
    ->ifNotExists()
    ->int('id')
    ->string('name')
    ->execute();

$db->insert('test')
    ->values([
        'id' => 1,
        'name' => 'test'
    ])
    ->execute();

print_r($db->select('test')->execute()->columns());
