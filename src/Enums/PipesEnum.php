<?php

namespace UniForceMusic\PHPDuckDBCLI\Enums;

enum PipesEnum: int
{
    case STDIN = 0;
    case STDOUT = 1;
    case STDERR = 2;
}
