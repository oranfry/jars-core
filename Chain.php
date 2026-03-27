<?php

namespace OranFry\Jars\Core;

class Chain
{
    private string $basePath;
    private array $blocks = [];

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

    public function dataFile(string $blockVersion, ...$suffixes): string
    {
        return $this->jars->dataFile($this->basePath, $blockVersion, ...$suffixes);
    }

    public function getBlock(string $version): Block
    {
        if (!array_key_exists($version, $this->blocks)) {
            $this->blocks[$version] = (new Block($this, $version))
                ->assertExistence()
                ->load();
        }

        return $this->blocks[$version];
    }

    public function createBlock(Block $base): Block
    {
        $version = bin2hex(random_bytes(32));

        return $this->blocks[$version] = (new Block($this, $version))
            ->previous($base->version())
            ->height($base->height() + 1)
            ->save();
    }

    public function jars(): Jars
    {
        return $this->jars;
    }
}
