<?php

use UniForceMusic\PHPDuckDB\Connection;

include 'vendor/autoload.php';

$connection = new Connection('duckdb', 'database.db');

echo $connection->execute('SELECT * FROM information_schema.tables');
