<?php

namespace OranFry\Jars\Core;

use OranFry\Jars\Contract\Exception;

class Filesystem
{
    const DEFAULT_LIMIT = 500;

    private array $store = [];
    private string $tmpDir;
    private int $inMemory = 0;

    public function __clone()
    {
        $store = [];

        foreach (array_keys($this->store) as $file) {
            $store[$file] = clone $this->store[$file];
        }

        $this->store = $store;
    }

    public function __construct(string $tmpDir)
    {
        $this->tmpDir = $tmpDir;
    }

    public function __destruct()
    {
        foreach ($this->store as $file => $details) {
            if ($details->dirty) {
                error_log(spl_object_id($this) .  ' Lossy filesystem destruction: ' . $file);
            }
        }

        $this->unlinkTmpFiles();
    }

    public function cached(string $file)
    {
        return array_key_exists($file, $this->store);
    }

    public function delete(string $file, int $priority = 100)
    {
        $this->put($file, null, false, $priority);
    }

    public function forget(string $file): self
    {
        if (!@$this->store[$file]->dirty) {
            if ($this->store[$file]->content !== null) {
                $this->inMemory--;
            }

            unset($this->store[$file]);
        }

        return $this;
    }

    public static function generateErrorMessage(string $filename, string $what): string
    {
        return "Could not file_put_contents [$filename], failed to $what";
    }

    public function get(string $file, bool $binary = false)
    {
        if ($tmpFile = $this->store[$file]->tmpFile ?? null) {
            if (defined('JARS_VERBOSE') && JARS_VERBOSE) {
                error_log('Pulling back from tmp space [' . $file . ']');
            }

            $this->promptOffload();

            $this->store[$file]->content = match (true) {
                $tmpFile === ':delete' => null,
                $binary => file_get_contents($tmpFile),
                default => json_decode(file_get_contents($tmpFile)),
            };

            $this->store[$file]->tmpFile = null;
            $this->inMemory++;

            if ($tmpFile !== ':delete') {
                unlink($tmpFile);
            }
        }

        if (!$this->cached($file)) {
            $this->promptOffload();
            $this->load($file, $binary);
        }

        return $this->store[$file]->content;
    }

    public function has(string $file)
    {
        if ($this->cached($file)) {
            $tmpFile = $this->store[$file]->tmpFile ?? null;

            return $this->store[$file]->content !== null || ($tmpFile && $tmpFile !== ':delete');
        }

        return is_file($file);
    }

    public function limit(): int
    {
        if (defined('JARS_FILESYSTEM_LIMIT')) {
            return JARS_FILESYSTEM_LIMIT;
        }

        return self::DEFAULT_LIMIT;
    }

    public function list(string $dir): array
    {
        $files = [];

        if (is_dir($dir)) {
            $handle = opendir($dir);

            while ($basename = readdir($handle)) {
                if ($basename !== '.' && $basename !== '..') {
                    $files[$basename] = true;
                }
            }
        }

        foreach ($this->store as $file => $details) {
            if (!preg_match('@^' . preg_quote($dir, '@') . '/([^/]+)$@', $file, $matches)) {
                continue;
            }

            $basename = $matches[1];

            $tmpFile = $details->tmpFile;

            if ($details->content !== null || ($tmpFile && $tmpFile !== ':delete')) {
                $files[$basename] = true;
            } elseif (isset($files[$basename])) {
                unset($files[$basename]);
            }
        }

        $files = array_keys($files);

        sort($files);

        return $files;
    }

    private function load($file, bool $binary)
    {
        $content = null;

        if (is_file($file)) {
            $fileContents = file_get_contents($file);
            $content = $binary ? $fileContents : json_decode(trim($fileContents));
        }

        if ($content !== null) {
            $this->inMemory++;
        }

        $this->store[$file] = (object) [
            'content' => $content,
            'dirty' => false,
            'tmpFile' => null,
            'binary' => $binary,
        ];
    }

    private function offload(): void
    {
        $before = memory_get_usage();

        if (defined('JARS_VERBOSE') && JARS_VERBOSE) {
            error_log('Memory usage before: ' . Jars::numberToSiSuffix($before) . 'B');
        }

        $saving = 0;

        foreach ($this->store as $file => $details) {
            if (!$details->dirty) {
                unset($this->store[$file]);
                $this->inMemory--;
                continue;
            }

            if ($details->tmpFile) {
                continue;
            }

            if ($details->content === null) {
                $details->tmpFile = ':delete';
            } else {
                $tmpFile = $this->newTmpFile();
                $dirname = dirname($tmpFile);

                if (!is_dir($dirname) && !mkdir($dirname, 0777, true)) {
                    throw new Exception($this->generateErrorMessage($tmpFile, 'mkdir tmp'));
                }

                if (!$handle = fopen($tmpFile, 'w')) {
                    throw new Exception($this->generateErrorMessage($tmpFile, 'fopen'));
                }

                $export = $details->binary ? $details->content : json_encode($details->content, JSON_UNESCAPED_SLASHES);
                $saving += strlen($export);

                if (fwrite($handle, $export) === false) {
                    throw new Exception($this->generateErrorMessage($tmpFile, 'fwrite'));
                }

                if (!fsync($handle)) {
                    throw new Exception($this->generateErrorMessage($tmpFile, 'fsync'));
                }

                if (!fclose($handle)) {
                    throw new Exception($this->generateErrorMessage($tmpFile, 'fclose'));
                }

                $details->tmpFile = $tmpFile;

                $this->inMemory--;
            }

            $details->content = null;
        }

        if (defined('JARS_VERBOSE') && JARS_VERBOSE) {
            error_log('Offloaded ' . Jars::numberToSiSuffix($saving) . 'B to disk');

            $diff = $before - ($after = memory_get_usage());
            error_log('Memory usage after: ' . Jars::numberToSiSuffix($after) . 'B (saved ' . Jars::numberToSiSuffix($diff) . 'B)');
        }
    }

    private function newTmpFile(): string
    {
        $random = bin2hex(random_bytes(8));

        return $this->tmpDir . '/' . substr($random, 0, 2) . '/' . substr($random, 2, 2) . '/' . substr($random, 4);
    }

    public function persist(): self
    {
        $this->offload();

        $dirtyByPriority = [];
        $workDone = ['add' => 0, 'delete' => 0];

        foreach ($this->store as $file => $details) {
            if (!$details->dirty) {
                continue;
            }

            $dirtyByPriority[$details->priority][$file] = $details;
        }

        ksort($dirtyByPriority);

        foreach ($dirtyByPriority as $priority => $subStore) {
            foreach ($subStore as $file => $details) {
                if ($details->tmpFile === ':delete') {
                    if (is_file($file)) {
                        unlink($file);
                        $workDone['delete']++;
                    }
                } else {
                    $dirname = dirname($file);

                    if (!is_dir($dirname) && !mkdir($dirname, 0777, true)) {
                        throw new Exception($this->generateErrorMessage($file, 'mkdir'));
                    }

                    if (!rename($details->tmpFile, $file)) {
                        throw new Exception($this->generateErrorMessage($file, 'rename'));
                    }

                    $workDone['add']++;
                }

                // $details->tmpFile = null;
                // $details->dirty = false;
                unset($this->store[$file]);
            }
        }

        if (defined('JARS_VERBOSE') && JARS_VERBOSE && ($workDone['delete'] + $workDone['add'])) {
            $message = "Persisted changes to filesystem ";
            $message .= implode(' ', array_map(fn ($action) => match ($action) { 'add' => '+', 'delete' => '-' } . $workDone[$action] , array_keys(array_filter($workDone))));

            error_log($message);
        }

        return $this;
    }

    private function promptOffload(): self
    {
        if ($this->inMemory >= $this->limit()) {
            if (defined('JARS_VERBOSE') && JARS_VERBOSE) {
                error_log("Filesystem swollen with [$this->inMemory] files, time to offload to disk");
            }

            $this->offload();

            if (defined('JARS_VERBOSE') && JARS_VERBOSE && $this->inMemory !== 0) {
                error_log('inMemory not zero after offload [' . $this->inMemory . ']');
            }

            $this->inMemory = 0;
        }

        return $this;
    }

    public function put(string $file, $content, bool $binary = false, int $priority = 100)
    {
        if (@$this->store[$file]->content !== null) {
            $this->inMemory--;
        }

        if ($oldTmpFile = @$this->store[$file]->tmpFile) {
            unlink($oldTmpFile);
        }

        $this->store[$file] = (object) [
            'binary' => $binary,
            'content' => $content,
            'dirty' => true,
            'priority' => $priority,
            'tmpFile' => null,
        ];

        if ($this->store[$file]->content !== null) {
            $this->inMemory++;
        }

        $this->promptOffload();
    }

    public function reset(): self
    {
        $this->unlinkTmpFiles();

        $this->store = [];
        $this->inMemory = 0;

        return $this;
    }

    public function revert(string $file): self
    {
        if (@$this->store[$file]->content !== null) {
            $this->inMemory--;
        }

        unset($this->store[$file]);

        return $this;
    }

    private function unlinkTmpFiles(): self
    {
        foreach ($this->store as $file => $details) {
            if ($details->tmpFile !== null && $details->tmpFile !== ':delete') {
                @unlink($details->tmpFile);
            }
        }

        return $this;
    }
}
