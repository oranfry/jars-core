<?php

namespace OranFry\Jars\Core;

class Link
{
    private ?array $data = null;
    private bool $dirty = false;
    private ?string $file = null;
    private string $id;
    private Jars $jars;
    private string $name;
    private bool $reverse;
    private int $version;

    private function __construct(Jars $jars, string $name, int $version, string $id, bool $reverse)
    {
        $this->jars = $jars;
        $this->name = $name;
        $this->version = $version;
        $this->id = $id;
        $this->reverse = $reverse;
    }

    public function add(string $linked_id): self
    {
        if ($this->data === null) {
            $this->load();
        }

        if (!in_array($linked_id, $this->data)) {
            $this->data[] = $linked_id;
            $this->dirty = true;
        }

        return $this;
    }


    public function direction()
    {
        return $this->reverse ? 'back' : 'forth';
    }

    private function export(): array
    {
        if ($this->data === null) {
            $this->load();
        }

        return $this->data;
    }

    public function firstChild(): ?string
    {
        if ($this->data === null) {
            $this->load();
        }

        return $this->data[0] ?? null;
    }

    private function load(): self
    {
        $file = $this->readFile();

        $this->data = $file ? $this->jars->filesystem()->get($file) : [];

        return $this;
    }

    public function name()
    {
        return $this->name;
    }

    public static function of(Jars $jars, string $name, int $version, string $id, ?bool $reverse = null): self
    {
        $key = 'existing--' . $id . '--' . $name . '--' . $version . '--' . ($reverse ? 'back' : 'forth');

        return $jars->linkStore($key) ?? $jars->linkStore($key, new self($jars, $name, $version, $id, $reverse ?? false));
    }

    public static function propose(Jars $jars, string $name, int $version, string $id, ?bool $reverse = null): self
    {
        $key = 'proposed--' . $id . '--' . $name . '--' . $version . '--' . ($reverse ? 'back' : 'forth');

        if ($link = $jars->linkStore($key)) {
            return $link;
        }

        @unlink(self::writeFileOf($jars, $name, $version, $id, $reverse ?? false)); // first clean up any previous failed attempts to update this record

        return $jars->linkStore($key, new self($jars, $name, $version, $id, $reverse ?? false));
    }

    private function readFile(): ?string
    {
        if (null === $this->file) {
            $bestVersion = 0;
            $dir = dirname($this->jars->dataFile($this->id));

            foreach ($this->jars->filesystem()->list($dir) as $file) {
                if (!preg_match('/^' . $this->id . '\.l\.' . $this->direction() . '\.' . $this->name . '\.([0-9]+)$/', $file, $matches)) {
                    continue;
                }

                $version = intval($matches[1]);

                if ($version > $this->version) {
                    continue;
                }

                if ($version > $bestVersion) {
                    $bestVersion = $version;
                    $this->file = $dir . '/' . $file;
                }
            }
        }

        return $this->file;
    }

    public function relatives()
    {
        if ($this->data === null) {
            $this->load();
        }

        return $this->data;
    }

    public function remove(string $linked_id): self
    {
        if ($this->data === null) {
            $this->load();
        }

        if (in_array($linked_id, $this->data)) {
            // $this->data = array_values(array_diff($this->data, [$linked_id]));
            $this->data = array_values(array_filter($this->data, function ($e) use ($linked_id) {
                return $e != $linked_id;
            }));
            $this->dirty = true;
        }

        return $this;
    }

    public function save(): self
    {
        if ($this->dirty) {
            $this->jars->filesystem()->put(
                $this->writeFile(),
                $this->export(),
            );
        }

        return $this;
    }

    private function writeFile(): string
    {
        return static::writeFileOf($this->jars, $this->name, $this->version, $this->id, $this->reverse);
    }

    public static function writeFileOf(Jars $jars, string $name, int $version, string $id, bool $reverse): string
    {
        return $jars->dataFile(
            $id,
            'l',
            $reverse ? 'back' : 'forth',
            $name,
            $version,
        );
    }
}
