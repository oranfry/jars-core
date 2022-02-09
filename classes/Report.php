<?php

namespace jars;

abstract class Report
{
    const DEFAULT = [];

    private static $known = [];
    protected $filesystem;

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
            if (($key = index_of_object($report, 'id', 'is', $id)) !== null) {
                unset($report[$key]);
            }

            return array_values($report);
        }, null);
    }

    private function file($group)
    {
        return Config::get()->db_home . "/reports/{$this->name}/{$group}.json";
    }

    public function filesystem()
    {
        if (func_num_args()) {
            $filesystem = func_get_arg(0);

            if (!($filesystem instanceof Filesystem)) {
                error_response(__METHOD__ . ': argument should be instance of Filesystem');
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

    public static function load($token, Filesystem $filesystem, $name)
    {
        if (!isset(static::$known[$name]) || static::$known[$name]->filesystem() !== $filesystem) {
            $reportclass = @BlendsConfig::get($token, $filesystem)->reports[$name];

            if (!$reportclass) {
                error_response("No such report '{$name}'");
            }

            $report = new $reportclass();
            $report->filesystem($filesystem);
            $report->name = $name;

            static::$known[$name] = $report;
        }

        return static::$known[$name];
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
            if (($key = index_of_object($report, 'id', 'is', $line->id)) !== null) {
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
        if (!$db_home = @Config::get()->db_home) {
            error_response('db_home not defined', 500);
        }

        $min_version_file = $db_home . '/versions/' . $min_version;

        if (!file_exists($min_version_file)) {
            error_response('No such version');
        }

        $current_version = file_get_contents($db_home . "/reports/version.dat");
        $min_version_num = (int) file_get_contents($min_version_file);
        $current_version_number = (int) file_get_contents($db_home . '/versions/' . $current_version);

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
            error_response('Invalid minimum version');
        }

        $tries = 10;

        while (!$this->version_requirement_met($min_version, 1000000) && --$tries);

        if (!$tries) {
            error_response('Version Timeout');
        }
    }
}
