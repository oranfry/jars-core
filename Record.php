<?php

namespace jars;

use jars\contract\Exception;

class Record
{
    private ?array $data = null;
    private bool $dirty = false;
    private ?string $extension = 'json';
    private Jars $jars;
    private ?string $format = 'json';
    private ?string $id = null;
    private string $table;

    public function __construct(Jars $jars, string $table, ?string $id = null)
    {
        $this->jars = $jars;

        if ($tableinfo = $this->jars->config()->tables()[$table] ?? null) {
            $this->format = @$tableinfo->format;
            $this->extension = @$tableinfo->extension;
        }

        $this->id = $id;
        $this->table = $table;

        if ($this->id === null) {
            $this->data = [];
        }
    }

    public function __get(string $property)
    {
        if ($property == 'id' ) {
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

        if ($property == 'id') {
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
        if (!$this->exists()) {
            throw new Exception("import_r: No such record: {$this->table}/{$this->id}");
        }
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
        return $this->jars->filesystem()->has($this->file());
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
            throw new Exception('Could not generate filename');
        }

        return $this->jars->db_path('records/' . $this->table . '/' . $this->id . ($this->extension ? '.' . $this->extension : null));
    }

    private function load()
    {
        $this->assertExistence();

        $file = $this->file();
        $content = $this->jars->filesystem()->get($file);

        if ($this->format == 'binary') {
            $this->data = ['content' => $content];
        } else {
            $this->data = json_decode($content, true);

            if (!is_array($this->data)) {
                throw new Exception('Invalid JSON found in a record file [' . $file . ']');
            }
        }
    }

    public static function of(Jars $jars, string $table, ?string $id = null)
    {
        return new Record($jars, $table, $id);
    }

    public function save()
    {
        if ($this->id === null) {
            throw new Exception('Tried to save record without id');
        }

        if ($this->data === null || !$this->dirty) {
            return;
        }

        $this->jars->filesystem()->put(
            $this->file(),
            $this->export(),
        );
    }

    public function toArray(): array
    {
        if ($this->data === null) {
            $this->load();
        }

        return array_merge(['id' => $this->id], $this->data);
    }
}
