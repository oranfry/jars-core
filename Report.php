<?php

namespace OranFry\Jars\Core;

use OranFry\Jars\Contract\Constants;
use OranFry\Jars\Contract\Exception;
use OranFry\Jars\Contract\VersionTimeoutException;

abstract class Report
{
    // php -r '$_wt = [1, 10, 10, 10, 10, 10, 100, 200, 200, 500]; echo "    const VERSION_WAIT_TRIES = [" . implode(", ", array_map(fn ($try) => $try / array_sum($_wt), $_wt)) . "];\n";'

    protected $filesystem;
    protected $jars;
    public $classify;
    public $listen = [];
    public $name;
    public $sorter;

    private function file(string $group): string
    {
        $basename = $group ?: '_empty';

        return $this->jars->db_path("reports/$this->name/$basename.json");
    }

    public function filesystem(?Filesystem $filesystem = null): null|Filesystem|self
    {
        if (func_num_args()) {
            $this->filesystem = $filesystem;

            return $this;
        }

        return $this->filesystem;
    }

    public function has(string $group): bool
    {
        if (!preg_match('/^' . Constants::GROUP_PATTERN . '$/', $group)) {
            throw new Exception('Invalid group');
        }

        return $this->filesystem->has($this->file($group));
    }

    public function is_derived(): bool
    {
        foreach ($this->listen as $key => $value) {
            $linetype = is_numeric($key) ? $value : $key;

            if (preg_match('/^report:/', $linetype)) {
                return true;
            }
        }

        return false;
    }

    public function is_fully_derived(): bool
    {
        foreach ($this->listen as $key => $value) {
            $linetype = is_numeric($key) ? $value : $key;

            if (!preg_match('/^report:/', $linetype)) {
                return false;
            }
        }

        return true;
    }

    public function jars(?Jars $jars = null): null|Jars|self
    {
        if (func_num_args()) {
            $this->jars = $jars;

            return $this;
        }

        return $this->jars;
    }

    public function linetypes()
    {
        if ($this->is_derived()) {
            return [];
        }

        $linetypes = [];

        foreach ($this->listen as $key => $value) {
            $linetypes[] = is_numeric($key) ? $value : $key;
        }

        return $linetypes;
    }

    public function listen(): array
    {
        $keys = array_map(fn ($key, $value): string => is_numeric($key) ? $value : $key, array_keys($this->listen), $this->listen);

        $values = array_map(function ($key, $value): object {
            if (is_numeric($key)) {
                return (object) [];
            }

            return $value;
        }, array_keys($this->listen), $this->listen);

        return array_combine($keys, $values);
    }
}
