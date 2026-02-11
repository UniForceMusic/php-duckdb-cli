<?php

namespace UniForceMusic\PHPDuckDBCLI\Integrations\PHP\SQLite3;

use UniForceMusic\PHPDuckDBCLI\PreparedStatement;

class SQLite3Stmt extends \SQLite3Stmt
{
    protected array $params = [];

    public function __construct(protected SQLite3 $sqlite3, protected string $sql)
    {
    }

    public function bindParam($param, &$var, $type = SQLITE3_TEXT)
    {
        $this->params[$param] = $var;
    }

    public function bindValue($param, $value, $type = SQLITE3_TEXT)
    {
        $this->params[$param] = $value;
    }

    public function clear()
    {
        return;
    }

    public function close()
    {
        return;
    }

    public function execute(): SQLite3Result|bool
    {
        return $this->sqlite3->query($this->getSQL(true));
    }

    public function getSQL($expand = false): string
    {
        if (!$expand) {
            return $this->sql;
        }

        return (new PreparedStatement($this->sql, $this->params))->toSql();
    }

    public function paramCount(): int
    {
        return count($this->params);
    }

    public function readOnly()
    {
        return;
    }

    public function reset()
    {
        return;
    }
}
