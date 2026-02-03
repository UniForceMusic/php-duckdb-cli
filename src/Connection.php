<?php

namespace UniForceMusic\PHPDuckDBCLI;

use Throwable;
use UniForceMusic\PHPDuckDBCLI\Mode;
use UniForceMusic\PHPDuckDBCLI\Exceptions\ConnectionException;
use UniForceMusic\PHPDuckDBCLI\Exceptions\DuckDBException;
use UniForceMusic\PHPDuckDBCLI\Results\EmptyResult;
use UniForceMusic\PHPDuckDBCLI\Results\ResultInterface;

class Connection
{
    public const int PIPE_STDIN = 0;
    public const int PIPE_STDOUT = 1;
    public const int PIPE_STDERR = 2;
    public const string INI_PCRE_JIT = 'pcre.jit';
    public const string REGEX_RESULT_OUTPUT = '/([\S\s]*?)\s*changes\:\s*[0-9]+\s*total_changes\:\s*[0-9]+\s*$/m';
    public const string REGEX_ERROR_OUTPUT = '/^([A-Za-z\-\_\s]*Error\:?[\S\s]+)$/im';

    private mixed $process;
    private array $pipes = [];
    private ?int $timeout = null;

    public function __construct(string $binary, ?string $file, bool $readOnly, private Mode $mode)
    {
        $command = sprintf(
            '%s -noheader',
            escapeshellarg($binary)
        );

        if ($readOnly) {
            $command .= ' -readonly';
        }

        if ($mode == Mode::JSON) {
            $command .= ' -json';
        }

        if ($file) {
            $command .= ' ';
            $command .= escapeshellarg($file);
        }

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

        $this->execute('.changes on', false, false);

        $this->execute(
            sprintf(
                '.maxrows %d',
                PHP_INT_MAX
            ),
            false,
            false
        );

        $this->execute(
            sprintf(
                '.maxwidth %d',
                PHP_INT_MAX
            ),
            false,
            false
        );
    }

    public function removeTimeout(): void
    {
        $this->timeout = null;
    }

    public function setTimeout(int $microseconds): void
    {
        $this->timeout = $microseconds;
    }

    public function changeMode(Mode $mode): void
    {
        $this->mode = $mode;

        $this->execute(
            sprintf(
                '.mode %s',
                $mode->value
            ),
            false,
            false
        );
    }

    public function execute(string $sql, bool $addSemicolon = true, bool $expectResult = true): ResultInterface
    {
        $startTime = microtime(true);

        while (true) {
            $this->throwExceptionIfTimedOut(
                $startTime,
                'waiting for remaining output to finish streaming'
            );

            $streams = $this->readStdout() . $this->readStderr();

            if (empty($streams)) {
                break;
            }
        }

        if ($addSemicolon && !preg_match('/;\s*$/', $sql)) {
            $sql .= ';';
        }

        $this->writeStdin($sql);

        if (!$expectResult) {
            return new EmptyResult();
        }

        $output = '';

        while (true) {
            $this->throwExceptionIfTimedOut(
                $startTime,
                "executing '{$sql}'"
            );

            $streams = $this->readStdout() . $this->readStderr();

            if (!empty($streams)) {
                $output .= $streams;
            }

            if (!proc_get_status($this->process)['running']) {
                throw new DuckDBException('DuckDB process has quit unexpectedly');
            }

            [$resultOutput, $errorOutput] = $this->hasFinishedGeneratingOutput($output);

            if ($resultOutput !== false) {
                $output = $resultOutput;

                break;
            }

            if (!empty($errorOutput)) {
                throw new DuckDBException($errorOutput);
            }
        }

        return $this->parse($output);
    }

    private function throwExceptionIfTimedOut(float $startTime, string $reason): void
    {
        if (!$this->timeout) {
            return;
        }

        if ((microtime(true) - $startTime) * 10000 > $this->timeout) {
            throw new ConnectionException("connection timed out while {$reason}");
        }
    }

    private function hasFinishedGeneratingOutput(string $output): array
    {
        try {
            $ini = ini_get(static::INI_PCRE_JIT);

            ini_set(static::INI_PCRE_JIT, '0');

            if ((bool) preg_match(self::REGEX_RESULT_OUTPUT, $output, $match)) {
                return [$match[1], false];
            }

            if ((bool) preg_match(self::REGEX_ERROR_OUTPUT, $output, $match)) {
                return [false, $match[1]];
            }

            return [false, false];
        } catch (Throwable $exception) {
            throw $exception;
        } finally {
            if (!is_bool($ini)) {
                ini_set(static::INI_PCRE_JIT, $ini);
            }
        }
    }

    protected function writeStdin(string $string): void
    {
        fwrite($this->pipes[self::PIPE_STDIN], $string . PHP_EOL);
    }

    protected function readStdout(): string
    {
        return stream_get_contents($this->pipes[self::PIPE_STDOUT]);
    }

    protected function readStderr(): string
    {
        return stream_get_contents($this->pipes[self::PIPE_STDERR]);
    }

    protected function parse(string $output): ResultInterface
    {
        return $this->mode->getParser($output)->parse();
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
