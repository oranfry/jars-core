<?php

namespace OranFry\Jars\Core;

class FilePutter
{
    private string $tempfile;
    private string $filename;
    private ?string $data;
    private string $tmpDir;

    public function __construct(string $filename, ?string $data, string $tmpDir)
    {
        $this->filename = $filename;
        $this->data = $data;
        $this->tmpDir = $tmpDir;

        $this->tempfile = $this->tmpDir . '/' . bin2hex(random_bytes(8));
    }

    public function execute(): self
    {
        if (!rename($this->tempfile, $this->filename)) {
            throw new Exception($this->generateErrorMessage('rename'));
        }

        return $this;
    }

    public function prepare(): self
    {
        if (!is_dir($this->tmpDir) && !mkdir($this->tmpDir, 0777, true)) {
            throw new Exception($this->generateErrorMessage('mkdir tmp'));
        }

        $dirname = dirname($this->filename);

        if (!is_dir($dirname) && !mkdir($dirname, 0777, true)) {
            throw new Exception($this->generateErrorMessage('mkdir'));
        }

        if (!$handle = fopen($this->tempfile, 'w')) {
            throw new Exception($this->generateErrorMessage('fopen'));
        }

        if (fwrite($handle, $this->data) === false) {
            throw new Exception($this->generateErrorMessage('fwrite'));
        }

        if (!fsync($handle)) {
            throw new Exception($this->generateErrorMessage('fsync'));
        }

        if (!fclose($handle)) {
            throw new Exception($this->generateErrorMessage('fclose'));
        }

        $this->data = null;

        return $this;
    }

    public static function generateErrorMessage(string $what): string
    {
        return "Could not file_put_contents [$this->filename], failed to $what";
    }
}