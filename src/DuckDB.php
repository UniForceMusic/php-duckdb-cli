<?php

namespace UniForceMusic\PHPDuckDBCLI;

use Throwable;
use UniForceMusic\PHPDuckDBCLI\Mode;
use UniForceMusic\PHPDuckDBCLI\Results\ResultInterface;

class DuckDB
{
    public const string BINARY = 'duckdb';

    private Connection $connection;
    private bool $inTransation = false;

    public static function memory(Mode $mode = Mode::JSON, string $binary = self::BINARY): static
    {
        return new static(null, false, $mode, $binary);
    }

    public static function file(string $file, bool $readOnly = false, Mode $mode = Mode::JSON, string $binary = self::BINARY): static
    {
        return new static($file, $readOnly, $mode, $binary);
    }

    public function __construct(
        ?string $file = null,
        bool $readOnly = false,
        Mode $mode = Mode::JSON,
        string $binary = self::BINARY
    ) {
        $this->connection = new Connection($binary, $file, $readOnly, $mode);
    }

    public function removeTimeout(): void
    {
        $this->connection->removeTimeout();
    }

    public function setTimeout(int $microseconds): void
    {
        $this->connection->setTimeout($microseconds);
    }

    public function duckboxMode(): void
    {
        $this->connection->changeMode(Mode::DUCKBOX);
    }

    public function jsonMode(): void
    {
        $this->connection->changeMode(Mode::JSON);
    }

    public function dotCommand(string $command): void
    {
        $this->connection->execute($command, false, false);
    }

    public function exec(string $statement): void
    {
        $this->connection->execute($statement);
    }

    public function query(string $query): ResultInterface
    {
        return $this->connection->execute($query);
    }

    public function prepared(string $query, array $params = []): ResultInterface
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
}
