<?php

namespace UniForceMusic\PHPDuckDBCLI\Integrations\Sentience;

use Sentience\Database\Queries\Objects\Column;
use Sentience\Database\Queries\Objects\Raw;
use Sentience\Database\Queries\Query;
use Sentience\Database\Results\ResultInterface;

/**
 * @property DuckDBDatabase $database
 * @property DuckDBDialect $dialect
 */
class CreateTableQuery extends \Sentience\Database\Queries\CreateTableQuery
{
    public function execute(bool $emulatePrepare = false): ResultInterface
    {
        return $this->database->transaction(
            function () use ($emulatePrepare): ResultInterface {
                $sequences = $this->createSequences();

                foreach ($this->columns as $column) {
                    if (!array_key_exists($column->name, $sequences)) {
                        continue;
                    }

                    $column->type = 'INT64';
                    $column->default = Query::raw(
                        sprintf(
                            'nextval(%s)',
                            $this->dialect->escapeString($sequences[$column->name])
                        )
                    );
                    $column->generatedByDefaultAsIdentity = false;
                }

                return parent::execute($emulatePrepare);
            }
        );
    }

    protected function createSequences(): array
    {
        $sequences = [];

        $table = $this->getTable($this->table);

        foreach ($this->columns as $column) {
            if (!$this->isSerialColumn($column)) {
                continue;
            }

            $sequence = sprintf(
                '%s_%s_sequence',
                $table,
                $column->name
            );

            $query = $this->dialect->createSequence($this->ifNotExists, $sequence);

            $this->database->exec($query->toSql($this->dialect));

            $sequences[$column->name] = $sequence;
        }

        return $sequences;
    }

    protected function isSerialColumn(Column $column): bool
    {
        return $column->generatedByDefaultAsIdentity || str_contains(strtoupper($column->name), 'SERIAL');
    }

    protected function getTable(string|array|Raw $table): string
    {
        if ($table instanceof Raw) {
            return $table->sql;
        }

        if (is_array($table)) {
            return $this->getTable(end($table));
        }

        return $this->table;
    }
}
