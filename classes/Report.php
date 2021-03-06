<?php

namespace jars;

use Exception;

abstract class Report
{
    const DEFAULT = [];

    protected $filesystem;
    protected $jars;

    public $listen = [];

    public function delegate_handling($token, $table, $record, $oldrecord, $oldlinks) : void
    {
        if (!in_array($table, $this->tables)) {
            return;
        }

        $this->{"handle_" . $table}($token, $record, $oldrecord, $oldlinks);
    }

    public function delete(string $group, string $id)
    {
        return $this->manip($group, function($report) use ($id) {
            if (($key = array_search($id, array_map(fn ($line) => $line->id, $report))) !== false) {
                unset($report[$key]);
            }

            return array_values($report);
        }, null);
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

    public function groups(?string $min_version = null)
    {
        return $this->get('groups', $min_version);
    }

    public function has($group)
    {
        return $this->filesystem->has($this->file($group));
    }

    public function linetypes()
    {
        $linetypes = [];

        foreach ($this->listen as $key => $value) {
            $linetypes[] = is_numeric($key) ? $value : $key;
        }

        return $linetypes;
    }

    public function manip($group, $callback)
    {
        $report = $this->get($group);
        $report = $callback($report);

        $this->save($group, $report);

        return $report;
    }

    public function save($group, $report)
    {
        $export = json_encode($report, JSON_PRETTY_PRINT);
        $groupsfile = $this->file('groups');
        $reportfile = $this->file($group);
        $groups = json_decode($this->filesystem->get($groupsfile) ?? json_encode(static::DEFAULT));

        if ($export == json_encode(static::DEFAULT, JSON_PRETTY_PRINT)) {
            $this->filesystem->delete($reportfile);

            if (($key = array_search($group, $groups)) !== false) {
                unset($groups[$key]);
                $groups = array_values($groups);
                $this->filesystem->put($groupsfile, json_encode($groups, JSON_PRETTY_PRINT));
            }
        } else {
            $this->filesystem->put($reportfile, $export);

            if (!in_array($group, $groups)) {
                $groups[] = $group;
                sort($groups);
                $this->filesystem->put($groupsfile, json_encode($groups, JSON_PRETTY_PRINT));
            }
        }
    }

    public function upsert(string $group, object $line, ?callable $sorter = null)
    {
        return $this->manip($group, function ($report) use ($line, $sorter) {
            if (($key = array_search($line->id, array_map(fn ($line) => $line->id, $report))) !== false) {
                $report[$key] = $line;
            } else {
                $report[] = $line;
            }

            if ($sorter) {
                usort($report, $sorter);
            }

            return $report;
        }, []);
    }

    private function version_requirement_met(string $min_version, int $micro_delay = 0)
    {
        $min_version_file = $this->jars->db_path('versions/' . $min_version);

        if (!file_exists($min_version_file)) {
            throw new Exception('No such version');
        }

        $current_version = file_get_contents($this->jars->db_path("reports/version.dat"));
        $min_version_num = (int) file_get_contents($min_version_file);
        $current_version_number = (int) file_get_contents($this->jars->db_path('versions/' . $current_version));

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
            if ($met = $this->version_requirement_met($min_version, $try)) {
                break;
            }
        }

        if (!$met) {
            throw new Exception('Version Timeout');
        }
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
}
