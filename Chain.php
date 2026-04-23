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

    public function dataFile(string $blockVersion): string
    {
        return $this->jars->dataFile($this->basePath, $blockVersion);
    }

    public function getBlock(string $version): Block
    {
        if (!array_key_exists($version, $this->blocks)) {
            $this->blocks[$version] = (new Block($this, $version))
                ->load();
        }

        return $this->blocks[$version];
    }

    public function createBlock(Block $base, string $timestamp): Block
    {
        $version = bin2hex(random_bytes(32));

        return $this->blocks[$version] = (new Block($this, $version))
            ->previous($base->version())
            ->height($base->height() + 1)
            ->timestamp($timestamp);
    }

    public function jars(): Jars
    {
        return $this->jars;
    }

    public function blockOfRecord(string $recordId): ?Block
    {
        return $this->jars->blockOfRecord($recordId);
    }

    public function trigger(string $event, ...$arguments): void
    {
        $this->jars->trigger($event, ...$arguments);
    }
}
