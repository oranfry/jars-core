<?php

namespace OranFry\Jars\Core;

use OranFry\Jars\Contract\Exception;

class Reporter
{
    private string $basePath;
    private array $managers = [];
    private Jars $jars;

    public function __construct(Jars $jars, string $basePath)
    {
        $this->jars = $jars;
        $this->basePath = $basePath;
    }

    public function basePath(): string
    {
        return $this->basePath;
    }

    public function path(?string $extra = null): string
    {
        if ($extra !== null) {
            return $this->basePath . '/' . $extra;
        }

        return $this->basePath;
    }

    public function dataFile(string $reportName, string $id, ...$suffixes): string
    {
        return $this->jars->dataFile($this->basePath . '/' . $reportName, $id, ...$suffixes);
    }

    public function getManager(string $report): ReportManager
    {
        if (!array_key_exists($report, $this->managers)) {
            $this->managers[$report] = (new ReportManager($this, $report));
        }

        return $this->managers[$report];
    }
}
