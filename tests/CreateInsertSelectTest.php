<?php

namespace UniForceMusic\PHPDuckDBCLI\Tests;

class CreateInsertSelectTest extends TestCase
{
    public function testCreateTable(): void
    {
        $this->duckdb->exec('CREATE TABLE test (id INT32, name VARCHAR)');

        $result = $this->duckdb->query('SELECT * FROM test LIMIT 0');

        $columns = $result->columns();

        $this->assertEquals(
            [
                'id' => 'int32',
                'name' => 'varchar'
            ],
            $columns
        );
    }

    public function testInsert(): void
    {
        $this->duckdb->exec('CREATE TABLE test (id INT32, name VARCHAR)');

        $rowValues = [
            [1, 'John Doe'],
            [2, 'Jane Doe']
        ];

        foreach ($rowValues as $values) {
            $this->duckdb->setTimeout(10000);

            $this->duckdb->prepared(
                'INSERT INTO test (id, name) VALUES (?, ?)',
                $values
            );
        }

        $result = $this->duckdb->query('SELECT * FROM test');

        $count = count($result->rows());

        $this->assertEquals(2, $count);
    }

    public function testSelect(): void
    {
        $this->duckdb->exec('CREATE TABLE test (id INT32, name VARCHAR)');
        $this->duckdb->exec("INSERT INTO test (id, name) VALUES (1, 'John Doe')");

        $result = $this->duckdb->query('SELECT * FROM test LIMIT 1');

        $row = $result->rows()[0];

        $this->assertEquals(
            ['id' => 1, 'name' => 'John Doe'],
            $row
        );
    }
}
