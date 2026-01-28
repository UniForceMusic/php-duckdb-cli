<?php

namespace UniForceMusic\PHPDuckDBCLI;

use Throwable;

class DuckDB
{
    public const string BINARY = 'duckdb';

    private Connection $connection;
    private bool $inTransation = false;

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
        $this->connection->execute($command, false, false);
    }

    public function exec(string $statement): void
    {
        $this->connection->execute($statement);
    }

    public function query(string $query): Result
    {
        return $this->connection->execute($query);
    }

    public function prepared(string $query, array $params = []): Result
    {
        return $this->query((new PreparedStatement($query, $params))->toSql());
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
