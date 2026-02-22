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
    private array $preparedStatementHashes = [];

    public static function memory(Mode $mode = Mode::JSON, bool $safe = true, string $binary = self::BINARY): static
    {
        return new static(null, false, $mode, $safe, $binary);
    }

    public static function file(string $file, bool $readOnly = false, Mode $mode = Mode::JSON, bool $safe = true, string $binary = self::BINARY): static
    {
        return new static($file, $readOnly, $mode, $safe, $binary);
    }

    public function __construct(
        ?string $file = null,
        bool $readOnly = false,
        Mode $mode = Mode::JSON,
        bool $safe = true,
        string $binary = self::BINARY
    ) {
        $this->connection = new Connection($binary, $file, $readOnly, $mode, $safe);
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
        /**
         * The integration relies on a statement or query finishing with "changes: [0-9]+ total_changes: [0-9]+".
         */
        if (strtolower($command) == '.changes off') {
            return;
        }

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
        $preparedStatement = new PreparedStatement($query, $params);

        if (!preg_match('/^\s*SELECT/i', $query)) {
            return $this->query($preparedStatement->toSql());
        }

        $queryHash = md5($query);

        if (array_key_exists($queryHash, $this->preparedStatementHashes)) {
            return $this->query(
                sprintf(
                    'EXECUTE "%s"(%s)',
                    $queryHash,
                    $preparedStatement->getPreparedParams()
                )
            );
        }

        $this->preparedStatementHashes[$queryHash] = $query;

        return $this->query(
            sprintf(
                'PREPARE "%s" AS (%s); EXECUTE "%s"(%s)',
                $queryHash,
                $query,
                $queryHash,
                $preparedStatement->getPreparedParams()
            )
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
}
