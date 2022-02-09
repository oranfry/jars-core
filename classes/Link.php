<?php

namespace jars;

class Link
{
    private $data = null;
    private $dirty = false;
    private $id;
    private $jars;
    private $name;
    private $version;

    public function __construct(Jars $jars, string $name, string $id, ?bool $reverse = null, ?string $version = null)
    {
        $this->jars = $jars;
        $this->name = $name;
        $this->id = $id;
        $this->reverse = (bool) $reverse;
        $this->version = $version;
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
        $version_path = $this->version ? 'past/' . $this->version : 'current';

        return $this->jars->db_path($version_path . '/links/' . $this->name . '/' . $this->direction() . '/' . $this->id . '.json');
    }

    private function load()
    {
        $file = $this->file();

        $this->data = json_decode($this->jars->filesystem()->get($file) ?? '[]', true);
    }

    public function add($linked_id)
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

    public function remove($linked_id)
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

        $this->jars->filesystem()->put($this->file(), count($this->data) ? json_encode($this->data) : null);
    }

    public function name()
    {
        return $this->name;
    }

    public static function of(Jars $jars, string $name, string $id, ?bool $reverse = null, ?string $version = null)
    {
        return new static($jars, $name, $id, $reverse, $version);
    }
}
