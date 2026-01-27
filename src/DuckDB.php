<?php

namespace UniForceMusic\PHPDuckDBCLI;

use Throwable;

class DuckDB
{
    public const string BINARY = 'duckdb';
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
    private ?int $timeout = null;

    public static function memory(string $binary = self::BINARY): static
    {
        return new static(null, $binary);
    }

    public static function file(string $file, string $binary = self::BINARY): static
    {
        return new static($file, $binary);
    }

    public function __construct(
        private ?string $file = null,
        private string $binary = self::BINARY
    ) {
        $this->connection = new Connection($binary, $file);

        $this->initConnection();
    }

    public function removeTimeout(): void
    {
        $this->connection->removeTimeout();
    }

    public function setTimeout(int $microseconds): void
    {
        $this->connection->setTimeout($microseconds);
    }

    public function dotCommand(string $command): void
    {
        $this->exec($command, false, false);
    }

    public function exec(string $statement, bool $addSemicolon = true, bool $expectResult = true): void
    {
        $this->connection->execute($statement, $addSemicolon, $expectResult);
    }

    public function query(string $query, bool $addSemicolon = true, bool $expectResult = true): ?Result
    {
        return $this->connection->execute($query, $addSemicolon, $expectResult);
    }

    public function prepared(string $query, array $params = [], bool $addSemicolon = true, bool $expectResult = true): ?Result
    {
        return $this->query(
            (new PreparedStatement($query, $params))->toSql(),
            $addSemicolon,
            $expectResult
        );
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

    private function initConnection(): void
    {
        $this->dotCommand('.changes on');

        $this->dotCommand(
            sprintf(
                '.maxrows %d',
                PHP_INT_MAX
            )
        );

        $this->dotCommand(
            sprintf(
                '.maxwidth %d',
                PHP_INT_MAX
            )
        );
    }
}
