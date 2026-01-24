<?php

namespace UniForceMusic\PHPDuckDB;

use UniForceMusic\PHPDuckDB\Exceptions\ConnectionException;

class Connection
{
    public const string REGEX_RESULT_PATTERN = '/^\┌[\─\┬]+\┐[\S\s]*\└[\─\┴]+\┘\s*D?$/m';
    public const string REGEX_ERROR_PATTERN = '/^[A-Za-z]+\sError\:[\S\s]+D?$/m';
    public const int PIPE_STDIN = 0;
    public const int PIPE_STDOUT = 1;
    public const int PIPE_STDERR = 2;
    public const int FREAD_SIZE = 8192;
    public const int USLEEP_TIME = 100;

    protected mixed $process;
    protected array $pipes = [];

    public function __construct(string $binary, string $file)
    {
        $command = sprintf(
            '%s -noheader %s',
            escapeshellarg($binary),
            escapeshellarg($file)
        );

        $this->process = proc_open(
            $command,
            [
                ['pipe', 'r'],
                ['pipe', 'w'],
                ['pipe', 'w']
            ],
            $this->pipes
        );

        if (!is_resource($this->process)) {
            throw new ConnectionException('unable to open process');
        }

        foreach ($this->pipes as $pipe) {
            stream_set_blocking($pipe, false);
        }
    }

    public function execute(string $sql): Result|Error
    {
        if (!preg_match('/;\s*$/', $sql)) {
            $sql .= ';';
        }

        fwrite($this->pipes[self::PIPE_STDIN], $sql);
        fwrite($this->pipes[self::PIPE_STDIN], PHP_EOL);

        $output = '';

        while (true) {
            $output .= fread($this->pipes[self::PIPE_STDOUT], static::FREAD_SIZE);
            $output .= fread($this->pipes[self::PIPE_STDERR], static::FREAD_SIZE);

            $output = trim($output);

            if ($this->isFinishedGeneratingOutput($output)) {
                break;
            }

            usleep(static::USLEEP_TIME);
        }

        return $this->isError($output)
            ? new Error($output)
            : new Result($output);
    }

    protected function isFinishedGeneratingOutput(string $output): bool
    {
        return $this->isError($output)
            || $this->isResult($output)
            || (bool) preg_match('/\s*D\s*$/', $output);
    }

    protected function isResult(string $output): bool
    {
        return (bool) preg_match(static::REGEX_RESULT_PATTERN, $output);
    }

    protected function isError(string $output): bool
    {
        return (bool) preg_match(static::REGEX_ERROR_PATTERN, $output);
    }

    public function __destruct()
    {
        foreach ($this->pipes as $pipe) {
            fclose($pipe);
        }

        proc_terminate($this->process);
        proc_close($this->process);
    }
}
