<?php

namespace OranFry\Jars\Core;

use OranFry\Jars\Contract\Exception;

class Filesystem
{
    private array $store = [];
    private string $tmpDir;

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
    }

    public function cached(string $file)
    {
        return array_key_exists($file, $this->store);
    }

    public function delete(string $file, int $priority = 100)
    {
        $this->store[$file] = (object) [
            'content' => null,
            'dirty' => true,
            'priority' => $priority,
        ];
    }

    public function forget(string $file): self
    {
        if (!@$this->store[$file]->dirty) {
            unset($this->store[$file]);
        }

        return $this;
    }

    public function get(string $file, ?string $default = null)
    {
        if (!$this->cached($file)) {
            $this->load($file, $default);
        }

        return $this->store[$file]->content;
    }

    public function has(string $file)
    {
        if ($this->cached($file)) {
            return $this->store[$file]->content !== null;
        }

        return is_file($file);
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

            if ($details->content !== null) {
                $files[$basename] = true;
            } elseif (isset($files[$basename])) {
                unset($files[$basename]);
            }
        }

        $files = array_keys($files);

        sort($files);

        return $files;
    }

    private function load($file, ?string $default = null)
    {
        if (is_file($file)) {
            $content = file_get_contents($file);
        } else {
            $content = $default;
        }

        $this->store[$file] = (object) [
            'content' => $content,
            'dirty' => false,
        ];
    }

    public function persist(): self
    {
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
            $putters = [];
            $unlink = [];

            foreach ($subStore as $file => $details) {
                if ($details->content !== null) {
                    $putters[] = (new FilePutter($file, $details->content, $this->tmpDir))->prepare();
                    $workDone['add']++;
                } elseif (is_file($file)) {
                    $unlink[] = $file;
                    $workDone['delete']++;
                }
            }

            foreach ($putters as $putter) {
                $putter->execute();
            }

            foreach ($unlink as $file) {
                unlink($file);
            }
        }

        foreach ($this->store as $file => $details) {
            $details->dirty = false;
        }

        if (defined('JARS_VERBOSE') && JARS_VERBOSE && ($workDone['delete'] + $workDone['add'])) {
            $message = "Persisted changes to filesystem ";
            $message .= implode(' ', array_map(fn ($action) => match ($action) { 'add' => '+', 'delete' => '-' } . $workDone[$action] , array_keys(array_filter($workDone))));

            error_log($message);
        }

        return $this;
    }

    public function put(string $file, $content, int $priority = 100)
    {
        $this->store[$file] = (object) [
            'content' => $content,
            'dirty' => true,
            'priority' => $priority,
        ];
    }

    public function reset(): self
    {
        $this->store = [];

        return $this;
    }

    public function revert(string $file): self
    {
        unset($this->store[$file]);

        return $this;
    }
}
