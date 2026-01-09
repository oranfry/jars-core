<?php

namespace jars;

class Link
{
    private $data = null;
    private $dirty = false;
    private $id;
    private $jars;
    private $name;
    private $reverse;

    public function __construct(Jars $jars, string $name, string $id, ?bool $reverse = null)
    {
        $this->jars = $jars;
        $this->name = $name;
        $this->id = $id;
        $this->reverse = (bool) $reverse;
    }

    public function direction()
    {
        return $this->reverse ? 'back' : 'forth';
    }

    public function firstChild()
    {
        if ($this->data === null) {
            $this->load();
        }

        if (count($this->data)) {
            return $this->data[0];
        }

        return null;
    }

    public function relatives()
    {
        if ($this->data === null) {
            $this->load();
        }

        return $this->data;
    }

    private function file()
    {
        $numSubParts = 2;
        $subPartLength = 2;

        $subParts = [];

        for ($p = 0; $p < $numSubParts; $p++) {
            $subParts[] = substr($this->id, $subPartLength * $p, $subPartLength);
        }

        $subdir = implode('/', $subParts);

        return $this->jars->db_path('links/' . $this->name . '/' . $this->direction() . '/' . $subdir . '/' . $this->id . '.json');
    }

    private function load(): self
    {
        $file = $this->file();
        $json = $this->jars->filesystem()->get($file) ?? '[]';

        $this->data = json_decode($json, true);

        return $this;
    }

    public function add($linked_id): self
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

    public function remove($linked_id): self
    {
        if ($this->data === null) {
            $this->load();
        }

        if (in_array($linked_id, $this->data)) {
            $this->data = array_values(array_filter($this->data, function ($e) use ($linked_id) {
                return $e != $linked_id;
            }));

            $this->dirty = true;
        }

        return $this;
    }

    public function save()
    {
        if ($this->data === null || !$this->dirty) {
            return;
        }

        $this->jars->filesystem()->put(
            $this->file(),
            count($this->data) ? json_encode($this->data, JSON_UNESCAPED_SLASHES) : null,
        );
    }

    public function name()
    {
        return $this->name;
    }

    public static function of(Jars $jars, string $name, string $id, ?bool $reverse = null)
    {
        return new static($jars, $name, $id, $reverse);
    }
}
