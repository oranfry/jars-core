<?php

namespace OranFry\Jars\Core;

class FilePutter
{
    const TMP_DIR = '/tmp/file_putter';

    private string $mode;
    private string $tempfile;
    private string $filename;
    private ?string $data;

    public function __construct(string $filename, ?string $data, int $flags = 0)
    {
        $this->filename = $filename;
        $this->data = $data;
        $this->mode = $flags & FILE_APPEND ? 'a' : 'w';
        $this->tempfile = self::TMP_DIR . '/' . bin2hex(random_bytes(8));
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
        if (!is_dir(self::TMP_DIR) && !mkdir(self::TMP_DIR, 0777, true)) {
            throw new Exception($this->generateErrorMessage('mkdir tmp'));
        }

        $dirname = dirname($this->filename);

        if (!is_dir($dirname) && !mkdir($dirname, 0777, true)) {
            throw new Exception($this->generateErrorMessage('mkdir'));
        }

        if ($this->mode === 'a' && is_file($this->filename) && !copy($this->filename, $this->tempfile)) {
            throw new Exception($this->generateErrorMessage('copy'));
        }

        if (!$handle = fopen($this->tempfile, $this->mode)) {
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