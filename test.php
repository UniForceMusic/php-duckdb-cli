<?php

use UniForceMusic\PHPDuckDB\DuckDB;

include 'vendor/autoload.php';

$duckDB = new DuckDB('database.db');

$result = $duckDB->query('INSERT INTO test1 (id, name) VALUES (1, \'test1\')');

print_r([
    'columns' => $result->columns(),
    'rows' => $result->rows()
]);
