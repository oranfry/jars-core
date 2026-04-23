<?php

namespace OranFry\Jars\Core;

use OranFry\Jars\Contract\Exception;

class xRecord
{
    private ?array $data = null;
    private bool $dirty = false;
    private Block $block;
    private ?string $format = 'json';
    private ?string $id = null;
    private string $table;

    public function __construct(Block $block, string $table, string $id)
    {
        $this->block = $block;

        $tableinfo = $this->block->chain()->jars()->config()->tables()[$table] ?? null;

        if ($tableinfo && @$tableinfo->format) {
            $this->format = $tableinfo->format;
        }

        $this->id = $id;
        $this->table = $table;

        $this->block->setRecord($this->table, $this->id, $this);
    }

    public function __get(string $property)
    {
        if ($property === 'id') {
            return $this->id;
        }

        if ($this->data === null) {
            $this->load();
        }

        if (!array_key_exists($property, $this->data)) {
            return null;
        }

        return $this->data[$property];
    }

    public function __isset(string $property)
    {
        return $this->data !== null && isset($this->data[$property]);
    }

    public function __unset(string $property)
    {
        if ($this->data !== null) {
            unset($this->data[$property]);
        }
    }

    public function __set(string $property, $value)
    {
        if (!is_null($value) && !is_scalar($value)) {
            throw new Exception('Attempt made to save non-scalar data to a record');
        }

        if ($property === 'id') {
            if (!is_string($value)) {
                throw new Exception('Attempt made to save non-string id to a record');
            }

            $this->id = $value;

            return;
        }

        if ($this->data === null) {
            $this->load();
        }

        if (!array_key_exists($property, $this->data) || $this->data[$property] !== $value) {
            $this->data[$property] = $value;
            $this->dirty = true;
        }
    }

    public function __toString(): string
    {
        return $this->export();
    }

    public function assertExistence()
    {
        if (!$this->block->recordExists($this->table, $this->id)) {
            throw new Exception("No such record: $this->table/$this->id");
        }
    }

    public function currentContents(): ?string
    {
        if (!is_file($file = $this->file())) {
            return null;
        }

        if (false === $contents = file_get_contents($file)) {
            return null;
        }

        return $contents;
    }

    public function delete()
    {
        $this->jars->filesystem()->delete($this->file());
    }

    public function equals(self $another)
    {
        if (is_null($record_a) !== is_null($record_b)) {
            return false;
        }

        $a_keys = array_keys(get_object_vars($record_a));
        $b_keys = array_keys(get_object_vars($record_b));

        if (array_diff($a_keys, $b_keys) || array_diff($b_keys, $a_keys)) {
            return false;
        }

        foreach ($a_keys as $key) {
            if ($record_a->$key !== $record_b->$key) {
                return false;
            }
        }

        return true;
    }

    public function exists()
    {
        return is_file($this->file());
    }

    private function export()
    {
        if ($this->data === null) {
            $this->load();
        }

        if ($this->format == 'binary') {
            return $this->data['content'];
        }

        return json_encode($this->data, JSON_UNESCAPED_SLASHES);
    }

    private function file()
    {
        if (!$this->id) {
            throw new Exception('Could not generate record filename - no id');
        }

        return $this->block->path('r.' . $this->table . '.' . $this->id);
    }

    public function data(?array $data = null): array|null|self
    {
        if (func_num_args()) {
            $this->data = $data;

            return $this;
        }

        return $this->data;
    }

    private function load()
    {
        $this->assertExistence();

        if (!is_file($file = $this->file())) {
            throw new Exception('Failed to load record from file [' . $file . ']');
        }

        $content = file_get_contents($file);

        if ($this->format == 'binary') {
            $this->data = ['content' => $content];
        } else {
            $this->data = json_decode($content, true);

            if (!is_array($this->data)) {
                throw new Exception('Invalid JSON found in a record file [' . $file . ']');
            }
        }
    }

    public static function of(Block $block, string $table, string $id)
    {
        return new Record($block, $table, $id);
    }

    public function save()
    {
        if ($this->id === null) {
            throw new Exception('Tried to save record without id');
        }

        if ($this->data === null || !$this->dirty) {
            return;
        }

        // file_put_contents($this->file(), $this->export());
    }

    public function version(): string
    {
        return $this->block->version();
    }

    public function block(?Block $block = null): self|Block|null
    {
        if (func_num_args()) {
            $this->block = $block;

            return $this;
        }

        return $this->block;
    }

    public function toArray(): array
    {
        if ($this->data === null) {
            $this->load();
        }

        return array_merge(['id' => $this->id], $this->data);
    }

    public function move(Block $block): self
    {
        $this->block = $block->setRecord($table, $id, $this);

        return $this;
    }

    public function init(): self
    {
        $this->data = [];

        return $this;
    }
}
