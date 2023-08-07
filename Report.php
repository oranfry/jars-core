<?php

namespace jars;

use jars\contract\Constants;

abstract class Report
{
    const DEFAULT = [];

    protected $filesystem;
    protected $jars;
    public $classify;
    public $listen = [];
    public $name;
    public $sorter;

    public function delegate_handling($token, $table, $record, $oldrecord, $oldlinks) : void
    {
        if (!in_array($table, $this->tables)) {
            return;
        }

        $this->{"handle_" . $table}($token, $record, $oldrecord, $oldlinks);
    }

    public function delete(string $group, string $linetype, string $id)
    {
        return $this->manip($group, function($report) use ($id, $linetype) {
            $key = array_search($linetype . '/' . $id, array_map(fn ($line) => $line->type . '/' . $line->id, $report));

            if ($key !== false) {
                unset($report[$key]);
            }

            return array_values($report);
        });
    }

    private function file($group)
    {
        return $this->jars->db_path("reports/{$this->name}/{$group}.json");
    }

    public function filesystem()
    {
        if (func_num_args()) {
            $filesystem = func_get_arg(0);

            if (!($filesystem instanceof Filesystem)) {
                throw new Exception(__METHOD__ . ': argument should be instance of Filesystem');
            }

            $prev = $this->filesystem;
            $this->filesystem = $filesystem;

            return $prev;
        }

        return $this->filesystem;
    }

    public function get(string $group, ?string $min_version = null)
    {
        if ($min_version !== null) {
            $this->wait_for_version($min_version);
        }

        if (!$diskContent = $this->filesystem->get($this->file($group))) {
            return static::DEFAULT;
        }

        return json_decode($diskContent);
    }

    public function groups(string $prefix = '', ?string $min_version = null): array
    {
        if (!preg_match('/^' . Constants::GROUP_PREFIX_PATTERN . '$/', $prefix)) {
            throw new Exception('Invalid prefix');
        }

        if ($min_version !== null) {
            $this->wait_for_version($min_version);
        }

        $groupsfile = $this->file($prefix . 'groups');

        return json_decode($this->filesystem->get($groupsfile) ?? '[]');
    }

    public function has($group)
    {
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

    public function jars()
    {
        if (func_num_args()) {
            $jars = func_get_arg(0);

            if (!($jars instanceof Jars)) {
                throw new Exception(__METHOD__ . ': argument should be instance of Jars');
            }

            $prev = $this->jars;
            $this->jars = $jars;

            return $prev;
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

    public function maintain_groups(string $group, bool $exists): void
    {
        $prefix = ($prefix = dirname($group)) !== '.' ? $prefix . '/' : '';
        $subgroup = basename($group);

        $this->maintain_groups_r($prefix, $subgroup, $exists);
    }

    public function maintain_groups_r(string $prefix, string $subgroup, bool $exists): void
    {
        $groupsfile = $this->file($prefix . 'groups');
        $groups = json_decode($this->filesystem->get($groupsfile) ?? '[]');

        if ($exists) {
            if (!in_array($subgroup, $groups)) {
                $groups[] = $subgroup;
                sort($groups);
            }
        } elseif (false !== $key = array_search($subgroup, $groups)) {
            unset($groups[$key]);
            $groups = array_values($groups);
        }

        $export = json_encode($groups, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES);

        if ($exists = $export !== '[]') {
            $this->filesystem->put($groupsfile, $export);
        } else {
            $this->filesystem->delete($groupsfile);
        }

        $pattern = '/^(' . Constants::GROUP_PREFIX_PATTERN . ')(' . Constants::GROUP_SUB_PATTERN . ')\\/$/';

        if (preg_match($pattern, $prefix, $matches)) {
            $this->maintain_groups_r($matches[1], $matches[2], $exists);
        }
    }

    public function manip(string $group, $callback)
    {
        if (!preg_match('/^' . Constants::GROUP_PATTERN . '$/', $group)) {
            throw new Exception('Invalid group');
        }

        $data = $callback($this->get($group));

        $this->save($group, $data);

        return $data;
    }

    public function save(string $group, $data)
    {
        if (!preg_match('/^' . Constants::GROUP_PATTERN . '$/', $group)) {
            throw new Exception('Invalid group');
        }

        $export = json_encode($data, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES);
        $reportfile = $this->file($group);

        if ($exists = $export !== json_encode(static::DEFAULT, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES)) {
            $this->filesystem->put($reportfile, $export);
        } else {
            $this->filesystem->delete($reportfile);
        }

        $this->maintain_groups($group, $exists);
    }

    public function upsert(string $group, object $line, ?callable $sorter = null)
    {
        return $this->manip($group, function ($report) use ($line, $sorter) {
            $key = array_search($line->type . '/' . $line->id, array_map(fn ($line) => $line->type . '/' . $line->id, $report));

            if ($key !== false) {
                $report[$key] = $line;
            } else {
                $report[] = $line;
            }

            if ($sorter) {
                usort($report, $sorter);
            }

            return $report;
        });
    }

    private function version_requirement_met(string $min_version, int $micro_delay = 0, &$feedback = [])
    {
        $min_version_file = $this->jars->db_path('versions/' . $min_version);

        if (!file_exists($min_version_file)) {
            throw new Exception('No such version');
        }

        $current_version = $feedback['current_version'] = $this->filesystem->get($version_file = $this->jars->db_path("reports/version.dat"));

        $this->filesystem->forget($version_file);

        $min_version_num = $feedback['min_version_num'] = (int) $this->filesystem->get($min_version_file);
        $current_version_number = $feedback['current_version_number'] = (int) $this->filesystem->get($this->jars->db_path('versions/' . $current_version));

        if ($current_version_number >= $min_version_num) {
            return true;
        }

        if ($micro_delay) {
            usleep($micro_delay);
        }

        return false;
    }

    private function wait_for_version(string $min_version)
    {
        if (!preg_match('/^[a-f0-9]{64}$/', $min_version)) {
            throw new Exception('Invalid minimum version [' . $min_version . ']');
        }

        $tries = [
            10000,
            100000,
            100000,
            100000,
            100000,
            100000,
            1000000,
            2000000,
            2000000,
            5000000,
        ];

        foreach ($tries as $try) {
            if ($this->version_requirement_met($min_version, $try, $feedback)) {
                return;
            }
        }

        throw new Exception("Version timeout waiting for [$min_version]; still at $feedback[current_version]");
    }
}
