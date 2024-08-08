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

    public function put(string $file, $content)
    {
        if ($this->read_only) {
            throw new Exception('Attempt made to modify read-only filesystem');
        }

        $this->store[$file] = (object) [
            'content' => $content,
            'dirty' => true,
        ];
    }

    public function has(string $file)
    {
        if (array_key_exists($file, $this->store)) {
            return $this->store[$file]->content !== null;
        }

        return is_file($file);
    }

    public function delete(string $file)
    {
        if ($this->read_only) {
            throw new Exception('Attempt made to modify read-only filesystem');
        }

        $this->store[$file] = (object) [
            'content' => null,
            'dirty' => true,
        ];
    }

    public function cached(string $file)
    {
        return array_key_exists($file, $this->store);
    }

    public function get(string $file, string $default = null)
    {
        if (!array_key_exists($file, $this->store)) {
            $this->load($file, $default);
        }

        return $this->store[$file]->content;
    }

    public function persist() : object
    {
        if ($this->no_persist) {
            throw new Exception('Attempt made to persist to no-persist filesystem');
        }

        foreach ($this->store as $file => $details) {
            if (!$details->dirty) {
                continue;
            }

            if ($details->content !== null) {
                @mkdir(dirname($file), 0777, true);
                file_put_contents($file, $details->content);
            } elseif (is_file($file)) {
                unlink($file);
            }

            $details->dirty = false;
        }

        return $this;
    }

    public function reset(): object
    {
        $this->store = [];

        return $this;
    }

    public function revert(string $file): object
    {
        unset($this->store[$file]);

        return $this;
    }

    public function forget(string $file): object
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

    public function append(string $file, $content)
    {
        if ($this->read_only) {
            throw new Exception('Attempt made to modify read-only filesystem');
        }

        if (!isset($this->store[$file])) {
            $this->load($file);
        }

        if ($content === null) {
            return;
        }

        $this->store[$file]->content .= $content;
        $this->store[$file]->dirty = true;
    }

    private function load($file, $default = null)
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
}
