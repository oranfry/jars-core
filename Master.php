<?php

namespace OranFry\Jars\Core;

class Master
{
    private string $basePath;
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

    public function dataFile(string $blockVersion): string
    {
        return $this->jars->dataFile($this->basePath, $blockVersion);
    }

    public function write(string $baseVersion, string $newVersion, string $timestamp, string $log): self
    {
        $file = $this->dataFile($baseVersion);

        Helper::mkdir(dirname($file), $this->basePath);
        Helper::file_put_contents($file, $newVersion . ' ' . $timestamp . ' ' . $log);

        return $this;
    }

    public function jars(): Jars
    {
        return $this->jars;
    }
}
