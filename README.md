# PHP DuckDB CLI

PHP DuckDB CLI is a wrapper around the DuckDB CLI. Not everyone has the ability to enable or install FFI extensions on their system.
This library provides a simple solution by interfacing with the command line interface.

Because of the limitations of the command line interface, certain features like prepared statements are emulated.

## Setup guide

Install the package using the following command:

```
composer require uniforcemusic/php-duckdb-cli
```

Start by creating a new DuckDB instance:

```php
use UniForceMusic\PHPDuckDBCLI\DuckDB;

$duckdb = new DuckDB('database.db');

// Also possible to initialize using static methods
// DuckDB::file('/path/to/file');
// DuckDB::memory();
```

Argument 1 (file) can be null if you want to use an in memory database.

Argument 2 (binary) can be a custom path to the DuckDB binary.

## Executing statements

DuckDB CLI offers 3 ways to execute SQL statements

```php
$duckdb->exec(string $statement): void;
$duckdb->query(string $query): Result;
$duckdb->prepared(string $query, array $params = []): Result;
```

The result class has two methods:

```php
$result->columns(): array;
$result->rows(): array;
```

To save on performance the output will only be parsed once one of these methods is invoked.

!! The parameters are interpolated in the string, unlike real prepared statements, so beware. !!

## Transactions

Like PDO, DuckDB CLI offers 4 methods for managing transactions

```php
$duckdb->beginTransaction(): void;
$duckdb->commitTransaction(): void;
$duckdb->rollbackTransaction(): void;
$duckdb->inTransaction(): bool;
```

## Tests

To run the tests, run `composer test` in your console

## Notice

There may be response case that this library does not handle. To prevent the system hanging, you can set a timeout.

```php
$duckdb->setTimeout($microseconds): void;
$duckdb->removeTimeout(): void;
```

This project is not super actively maintained. The inspiration to build this abstraction came from my work on my [database abstraction](https://github.com/Sentience-Framework/database)

If anybody wants to clone this project and start a more sophisticated version, feel free!
