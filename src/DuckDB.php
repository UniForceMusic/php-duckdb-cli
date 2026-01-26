<?php

namespace UniForceMusic\PHPDuckDBCLI;

use DateTimeInterface;
use Throwable;
use UniForceMusic\PHPDuckDBCLI\Exceptions\DuckDBException;

class DuckDB
{
    public const array ESCAPE_CHARS = [
        '\\' => '\\\\',
        "\n" => '\\n',
        "\r" => '\\r',
        "\t" => '\\t',
        "\0" => '',
        "\b" => '\\b',
        "\x1A" => '\\x1A',
        "\f" => '\\f',
        "\v" => '\\v'
    ];

    private Connection $connection;
    private bool $inTransation = false;

    public function __construct(
        private ?string $file = null,
        private string $binary = 'duckdb'
    ) {
        $this->connection = new Connection($binary, $file);
    }

    public function exec(string $statement): void
    {
        $this->connection->execute($statement);
    }

    public function query(string $query): Result
    {
        $result = $this->connection->execute($query);

        if ($result instanceof Error) {
            throw new DuckDBException($result->getError());
        }

        return $result;
    }

    public function prepared(string $query, array $params = []): Result
    {
        $preparedStatement = new PreparedStatement($query, $params);

        $result = $this->connection->execute($preparedStatement->toSql());

        if ($result instanceof Error) {
            throw new DuckDBException($result->getError());
        }

        return $result;
    }

    public function beginTransation(): void
    {
        $this->exec('BEGIN TRANSACTION;');

        $this->inTransation = true;
    }

    public function commitTransation(): void
    {
        try {
            $this->exec('COMMIT;');
        } catch (Throwable $exception) {
            throw $exception;
        } finally {
            $this->inTransation = false;
        }
    }

    public function rollbackTransation(): void
    {
        try {
            $this->exec('ROLLBACK;');
        } catch (Throwable $exception) {
            throw $exception;
        } finally {
            $this->inTransation = false;
        }
    }

    public function inTransaction(): bool
    {
        return $this->inTransation;
    }

    public static function compileValueToSQL(null|bool|int|float|string|DateTimeInterface $value): string
    {
        if (is_null($value)) {
            return 'NULL';
        }

        if (is_bool($value)) {
            return var_export($value);
        }

        if (is_string($value)) {
            return "'" . strtr($value, [...static::ESCAPE_CHARS, "'" => "''"]) . "'";
        }

        if ($value instanceof DateTimeInterface) {
            return static::compileValueToSQL($value->format('Y-m-d H:i:s.uP'));
        }

        return (string) $value;
    }
}
