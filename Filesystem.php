<?php

namespace jars;

use jars\contract\Exception;

class Filesystem
{
    public const NO_PERSIST = 1 << 0;
    public const READ_ONLY = 1 << 1;
    public const AUTO_PERSIST = 1 << 2;

    private bool $auto_persist;
    private bool $no_persist;
    private bool $read_only;
    private array $store = [];

    public function __construct(int $options = 0)
    {
        $this->auto_persist = (bool) ($options & static::AUTO_PERSIST);
        $this->no_persist = (bool) ($options & static::NO_PERSIST);
        $this->read_only = (bool) ($options & static::READ_ONLY);
    }

    public function __destruct()
    {
        if ($this->auto_persist)  {
            $this->persist();
        } elseif (!$this->read_only) {
            foreach ($this->store as $file => $details) {
                if ($details->dirty) {
                    error_log(spl_object_id($this) .  ' Lossy filesystem destruction: ' . $file);
                }
            }
        }
    }

    public function __clone()
    {
        $store = [];

        foreach (array_keys($this->store) as $file) {
            $store[$file] = clone $this->store[$file];
        }

        $this->store = $store;
    }

    public function put(string $file, $content, int $priority = 100)
    {
        if ($this->read_only) {
            throw new Exception('Attempt made to modify read-only filesystem');
        }

        $this->store[$file] = (object) [
            'mode' => 'overwrite',
            'content' => $content,
            'dirty' => true,
            'priority' => $priority,
        ];
    }

    public function has(string $file)
    {
        if ($this->cached($file)) {
            switch ($this->store[$file]->mode) {
                case 'append':
                    if ($this->store[$file]->dirty) {
                        return true;
                    }

                    break;
                default:
                    return $this->store[$file]->content !== null;
            }
        }

        return is_file($file);
    }

    public function delete(string $file, int $priority = 100)
    {
        if ($this->read_only) {
            throw new Exception('Attempt made to modify read-only filesystem');
        }

        $this->store[$file] = (object) [
            'content' => null,
            'mode' => 'overwrite',
            'dirty' => true,
            'priority' => $priority,
        ];
    }

    public function cached(string $file)
    {
        return array_key_exists($file, $this->store);
    }

    public function get(string $file, ?string $default = null)
    {
        if (!$this->cached($file)) {
            $this->load($file, $default);
        } elseif ($this->store[$file]->mode === 'append') {
            $this->switch_mode($file);
        }

        return $this->store[$file]->content;
    }

    public function persist(): self
    {
        if ($this->no_persist) {
            throw new Exception('Attempt made to persist to no-persist filesystem');
        }

        $dirtyByPriority = [];
        $workDone = ['add' => 0, 'delete' => 0, 'append' => 0];

        foreach ($this->store as $file => $details) {
            if (!$details->dirty) {
                continue;
            }

            $dirtyByPriority[$details->priority][$file] = $details;
        }

        ksort($dirtyByPriority);

        foreach ($dirtyByPriority as $priority => $subStore) {
            foreach ($subStore as $file => $details) {
                if ($details->mode === 'append') {
                    @mkdir(dirname($file), 0777, true);
                    file_put_contents($file, $details->content, FILE_APPEND);
                    $details->content = null;
                    $workDone['append']++;
                } elseif ($details->content !== null) {
                    @mkdir(dirname($file), 0777, true);
                    file_put_contents($file, $details->content);
                    $workDone['add']++;
                } elseif (is_file($file)) {
                    unlink($file);
                    $workDone['delete']++;
                }

                $details->dirty = false;
            }
        }

        if (defined('JARS_VERBOSE') && JARS_VERBOSE && ($workDone['delete'] + $workDone['add'] + $workDone['append'])) {
            $message = "Persisted changes to filesystem ";
            $message .= implode(' ', array_map(fn ($action) => match ($action) { 'add' => '+', 'append' => '.', 'delete' => '-' } . $workDone[$action] , array_keys(array_filter($workDone))));

            error_log($message);
        }

        return $this;
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

    public function forget(string $file): self
    {
        if (!@$this->store[$file]->dirty) {
            unset($this->store[$file]);
        }

        return $this;
    }

    public function freeze()
    {
        $this->auto_persist = false;
        $this->no_persist = true;
        $this->read_only = true;
    }

    public function getStore()
    {
        foreach ($this->store as $details) {
            $details->content = json_decode(json_encode($details->content));
        }

        return $this->store;
    }

    public function append(string $file, $content, ?int $priority = null)
    {
        if ($this->read_only) {
            throw new Exception('Attempt made to modify read-only filesystem');
        }

        if (!$this->cached($file)) {
            $this->store[$file] = (object) [
                'content' => null,
                'dirty' => false,
                'mode' => 'append',
            ];
        } elseif ($this->store[$file]->mode !== 'append') {
            $this->switch_mode($file);
        }

        if (
            $priority !== null
            || @$this->store[$file]->priority === null
        ) {
            $this->store[$file]->priority = $priority ?? 100;
        }

        if ($content === null) {
            return;
        }

        $this->store[$file]->content .= $content;
        $this->store[$file]->dirty = true;
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
            'mode' => 'overwrite',
        ];
    }

    private function switch_mode(string $file): self
    {
        if (null === $appendage = $this->store[$file]->content) {
            return $this->revert($file);
        }

        $this->load($file);
        $this->store[$file]->content .= $appendage;

        return $this;
    }
}
