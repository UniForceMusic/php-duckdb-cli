<?php

namespace UniForceMusic\PHPDuckDBCLI\Tests;

use Throwable;

class TransactionsTest extends TestCase
{
    public function testTransactionCommit(): void
    {
        $exceptionWasThrown = false;

        try {
            $this->duckdb->beginTransation();
            $this->duckdb->exec('SELECT 1');
            $this->duckdb->commitTransation();
        } catch (Throwable $exception) {
            $exceptionWasThrown = true;
        }

        $this->assertFalse(
            $exceptionWasThrown,
            "exception was thrown while running transaction with commit"
        );
    }

    public function testTransactionRollback(): void
    {
        $exceptionWasThrown = false;

        try {
            $this->duckdb->beginTransation();
            $this->duckdb->exec('SELECT 1');
            $this->duckdb->rollbackTransation();
        } catch (Throwable $exception) {
            $exceptionWasThrown = true;
        }

        $this->assertFalse(
            $exceptionWasThrown,
            "exception was thrown while running transaction with rollback"
        );
    }
}
