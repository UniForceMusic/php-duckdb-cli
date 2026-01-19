<?php

namespace UniForceMusic\PHPDuckDB;

use UniForceMusic\PHPDuckDB\Exceptions\ConnectionException;

class Connection
{
    public const int PIPE_STDIN = 0;
    public const int PIPE_STDOUT = 1;
    public const int PIPE_STDERR = 2;

    protected $process;
    protected array $pipes = [];

    public function __construct(string $binary, string $file)
    {
        $command = sprintf(
            '%s %s',
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

        fwrite($this->pipes[static::PIPE_STDIN], '');
    }

    public function execute(string $sql): string
    {
        if (!preg_match('/\;\s*$/', $sql)) {
            $sql .= ';';
        }

        fwrite($this->pipes[static::PIPE_STDIN], $sql);

        $response = null;

        while (true) {
            $stdout = stream_get_contents($this->pipes[static::PIPE_STDOUT]);
            $stderr = stream_get_contents($this->pipes[static::PIPE_STDERR]);

            echo $stdout . $stderr;
            continue;

            if (empty($stdout . $stderr)) {
                if (!$response) {
                    usleep(1);
                    continue;
                }

                break;
            }

            if (is_null($response)) {
                $response = '';
            }

            $response .= $stdout;
            $response .= $stderr;
        }

        return $response;
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
