<?php

namespace UniForceMusic\PHPDuckDBCLI\Tests;

class CreateInsertSelectTest extends TestCase
{
    public function testCreateTable(): void
    {
        $this->duckdb->exec('CREATE TABLE test (id INT32, name VARCHAR)');
        $this->duckdb->exec("INSERT INTO test (id, name) VALUES (1, 'test')");

        $result = $this->duckdb->query('SELECT * FROM test LIMIT 1');

        $columns = $result->getColumns();

        if (!array_is_list($columns)) {
            $columns = array_keys($columns);
        }

        $this->assertEquals(
            ['id', 'name'],
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

        $count = count($result->getRows());

        $this->assertEquals(2, $count);
    }

    public function testSelect(): void
    {
        $this->duckdb->exec('CREATE TABLE test (id INT32, name VARCHAR)');
        $this->duckdb->exec("INSERT INTO test (id, name) VALUES (1, 'John Doe')");

        $result = $this->duckdb->query('SELECT * FROM test LIMIT 1');

        $row = $result->getRows()[0];

        $this->assertEquals(
            ['id' => 1, 'name' => 'John Doe'],
            $row
        );
    }
}
