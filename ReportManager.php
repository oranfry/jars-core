<?php

namespace OranFry\Jars\Core;

use OranFry\Jars\Contract\Exception;
use OranFry\Jars\Contract\Constants;
use OranFry\Jars\Contract\VersionTimeoutException;

class ReportManager
{
    use Lockable;

    const DEFAULT_VALUE = [];

    const DEFAULT_TIMEOUT = 100000000;

    const ENCODING_OPTIONS = JSON_UNESCAPED_SLASHES;

    const VERSION_WAIT_TRIES = [0.00095147478591817, 0.0095147478591817, 0.0095147478591817, 0.0095147478591817, 0.0095147478591817, 0.0095147478591817, 0.095147478591817, 0.19029495718363, 0.19029495718363, 0.47573739295909];

    private Reporter $reporter;
    private string $reportName;
    private ?string $version = null;

    public function __construct(Reporter $reporter, string $reportName)
    {
        $this->reporter = $reporter;
        $this->reportName = $reportName;
    }

    public function path(?string $extra = null): string
    {
        $folder = $this->reporter->path($this->reportName);

        if ($extra !== null) {
            return $folder . '/' . $extra;
        }

        return $folder;
    }

    public function lockFile(): string
    {
        return $this->reporter->path($this->reportName) . '._lock';
    }

    public function postUnlock(): void
    {
        // unlike a block, we need to be able to lock the report time and time again
        // remove the lock file so fopen can succeed next time around

        if (defined('JARS_VERBOSE') && JARS_VERBOSE) {
            $file = $this->lockFile();
            error_log("Removing [$file]");
        }

        unlink($this->lockFile());
    }

    public function version(): string
    {
        if ($this->version !== null) {
            return $this->version;
        }

        if (is_file($file = $this->version_file())) {
            return $this->version = file_get_contents($file);
        }

        return $this->version = Constants::ROOT_VERSION;
    }

    public function version_file(): string
    {
        return $this->path('._version');
    }

    private function version_requirement_met(string $min_version, int $micro_delay = 0, &$feedback = []): bool
    {
        $minBlock = $this->reporter->getBlock($min_version);

        if (null === $minBlock) {
            throw new Exception('No such version');
        }

        $info = $this->reporter->iterateBlocks($minBlock);

        if (!$info->final) {
            throw new Exception('Head not reachable from min block');
        }

        $currentVersion = $feedback['current_version'] = file_get_contents($this->version_file());
        $currentBlock = $this->reporter->getBlock($currentVersion);

        $currentNumber = $feedback['current_version_number'] = $currentBlock->height();
        $minNumber = $feedback['min_version_num'] = $minBlock->height();

        if ($currentNumber >= $minNumber) {
            return true;
        }

        if ($micro_delay) {
            usleep($micro_delay);
        }

        return false;
    }

    private function wait_for_version(string $min_version, int $timeout_microseconds): void
    {
        if (!preg_match('/^[a-f0-9]{64}$/', $min_version)) {
            throw new Exception('Invalid minimum version [' . $min_version . ']');
        }

        foreach (static::VERSION_WAIT_TRIES as $try) {
            if ($this->version_requirement_met($min_version, (int) ($try * $timeout_microseconds), $feedback)) {
                return;
            }
        }

        throw new VersionTimeoutException("Version timeout waiting for [$min_version]; still at $feedback[current_version]");
    }

    public function dataFile(string $id, ...$suffixes): string
    {
        return $this->reporter->dataFile($this->reportName, $id, ...$suffixes);
    }

    public function mkdir(): self
    {
        Helper::mkdir($this->path(), $this->reporter->basePath());

        return $this;
    }

    public function get(Filesystem $filesystem, string $group, ?string $min_version, int $timeout_microseconds)
    {
        if (!preg_match('/^' . Constants::GROUP_PATTERN . '$/', $group)) {
            throw new Exception('Invalid group');
        }

        if ($min_version !== null) {
            $this->wait_for_version($min_version, $timeout_microseconds);
        }

        $file = $this->path(($group ?: '_empty') . '.json');

        return json_decode($filesystem->get($file) ?? '[]');
    }

    public function groups(Filesystem $filesystem, string $prefix, ?string $min_version, ?int $timeout_microseconds): array
    {
        if (!preg_match('/^' . Constants::GROUP_PREFIX_PATTERN . '$/', $prefix)) {
            throw new Exception('Invalid prefix');
        }

        if ($min_version !== null) {
            $this->wait_for_version($min_version, $timeout_microseconds);
        }

        $file = $this->groupsfile($prefix);

        return json_decode($filesystem->get($file) ?? '[]');
    }

    private function groupsfile(string $prefix): string
    {
        return $this->path($prefix . 'groups');
    }

    public function manip(string $group, Filesystem $filesystem, $callback)
    {
        if (!preg_match('/^' . Constants::GROUP_PATTERN . '$/', $group)) {
            throw new Exception('Invalid group');
        }

        // if (VERBOSE) {
        //     var_dump('<<<<<<<<<<<<<<<<<<<<', $this->reportName . '/' . $group, $this->get($filesystem, $group), '>>>>>>>>>>>>>>>>>>>>');
        //     echo "\n\n";
        // }

        $data = $callback($this->get($filesystem, $group, null, 0));

        $this->save($group, $data, $filesystem);

        return $data;
    }

    public function save(string $group, $data, Filesystem $filesystem)
    {
        if (!preg_match('/^' . Constants::GROUP_PATTERN . '$/', $group)) {
            throw new Exception('Invalid group');
        }

        $export = json_encode($data, self::ENCODING_OPTIONS);
        $reportfile = $this->path(($group ?: '_empty') . '.json');

        if ($exists = $export !== json_encode(self::DEFAULT_VALUE, self::ENCODING_OPTIONS)) {
            $filesystem->put($reportfile, $export);
        } else {
            $filesystem->delete($reportfile);
        }

        $this->maintain_groups($group, $exists, $filesystem);
    }

    public function maintain_groups(string $group, bool $exists, Filesystem $filesystem): void
    {
        if (!preg_match('/^' . Constants::GROUP_PATTERN . '$/', $group)) {
            throw new Exception('Invalid group');
        }

        $prefix = in_array($prefix = dirname($group), ['.', '']) ? '' : $prefix . '/';
        $subgroup = basename($group);

        $this->maintain_groups_r($prefix, $subgroup, $exists, $filesystem);
    }

    public function maintain_groups_r(string $prefix, string $subgroup, bool $exists, Filesystem $filesystem): void
    {
        if (!preg_match('/^' . Constants::GROUP_PREFIX_PATTERN . '$/', $prefix)) {
            throw new Exception('Invalid prefix');
        }

        $groupsfile = $this->groupsfile($prefix);
        $groups = json_decode($filesystem->get($groupsfile) ?? '[]');

        if (null === $groups) {
            var_dump($groupsfile, $filesystem->get($groupsfile));
            die('--');
        }

        if ($exists) {
            if (!in_array($subgroup, $groups)) {
                $groups[] = $subgroup;
                sort($groups);
            }
        } elseif (false !== $key = array_search($subgroup, $groups)) {
            unset($groups[$key]);
            $groups = array_values($groups);
        }

        $export = json_encode($groups, static::ENCODING_OPTIONS);

        if ($exists = $export !== '[]') {
            $filesystem->put($groupsfile, $export);
        } else {
            $filesystem->delete($groupsfile);
        }

        $pattern = '/^(' . Constants::GROUP_PREFIX_PATTERN . ')(' . Constants::GROUP_SUB_PATTERN . ')\\/$/';

        if (preg_match($pattern, $prefix, $matches)) {
            $this->maintain_groups_r($matches[1], $matches[2], $exists, $filesystem);
        }
    }

    public function upsert(string $group, object $line, Filesystem $filesystem, ?callable $sorter = null)
    {
        return $this->manip($group, $filesystem, function ($report) use ($line, $sorter) {
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

    public function delete(string $group, string $linetype, string $id, Filesystem $filesystem)
    {
        return $this->manip($group, $filesystem, function($report) use ($id, $linetype) {
            $key = array_search($linetype . '/' . $id, array_map(fn ($line) => $line->type . '/' . $line->id, $report));

            if ($key !== false) {
                unset($report[$key]);
            }

            return array_values($report);
        });
    }
}
