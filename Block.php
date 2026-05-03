<?php

namespace OranFry\Jars\Core;

use OranFry\Jars\Contract\Exception;

class Block
{
    use Lockable;

    private Chain $chain;

    private string $version; // id of the block; like a commit hash
    private ?string $previous = null;
    private ?string $next = null;
    private ?int $height = null;
    private int $pointer = 1;
    private ?string $timestamp = null;
    private ?array $meta = null;
    private array $records = [];
    private bool $loaded = false;

    private array $links = [];

    public function __construct(Chain $chain, string $version)
    {
        $this->chain = $chain;
        $this->version = $version;
    }

    public function assertExistence(): self
    {
        if (!$this->chain->blockExists($this->version)) {
            throw new Exception("Block: no such version [$this->version]");
        }

        return $this;
    }

    public function path(): string
    {
        return $this->chain->dataFile($this->version);
    }

    public function getRecord(string $table, string $id): object
    {
        return $this->records[$id . '.' . $table];
    }

    public function setRecord(string $table, string $id, object $record): self
    {
        $this->records[$id . '.' . $table] = $record;

        return $this;
    }

    public static function linkKey(string $linkName, string $recordId, ?bool $reverse): string
    {
        return $recordId . '.' . $linkName . '.' . ($reverse ? 'back' : 'forth');
    }

    public function getLink(string $linkName, string $recordId, ?bool $reverse): array
    {
        return $this->links[self::linkKey($linkName, $recordId, $reverse)];
    }

    public function addLink(string $linkName, string $recordId, ?bool $reverse, string $relative): self
    {
        $key = self::linkKey($linkName, $recordId, $reverse);

        $this->links[$key] = array_values(array_unique(array_merge($this->links[$key] ?? [], [$relative])));

        return $this;
    }

    public function removeLink(string $linkName, string $recordId, ?bool $reverse, string $relative): self
    {
        $key = self::linkKey($linkName, $recordId, $reverse);

        $this->links[$key] = array_values(array_filter($this->links[$key] ?? [], fn ($value) => $value !== $relative));

        return $this;
    }

    public function recordIds(): array
    {
        return array_map(fn ($record) => $record->id, $this->records);
    }

    public function linkKeys(): array
    {
        return array_keys($this->links);
    }

    public function version(): string
    {
        return $this->version;
    }

    public function previous(?string $previous = null): self|string|null
    {
        if (func_num_args()) {
            if ($this->version === Jars::INITIAL_VERSION) {
                throw new Exception("Block: refusing to set a previous block against the initial block");
            }

            $this->previous = $previous;

            return $this;
        }

        if ($this->version === Jars::INITIAL_VERSION) {
            return null;
        }

        return $this->previous;
    }

    public function next(): ?string
    {
        return $this->next;
    }

    public function setNext(string $next): self
    {
        fwrite($this->lock, $this->next = $next);

        return $this;
    }

    public function load(): self
    {
        if (!$this->loaded) {
            $this->assertExistence();

            if ($this->version !== Jars::INITIAL_VERSION) {
                if (!is_file($this->path())) {
                    throw new Exception('boom 1');
                }

                $data = json_decode(file_get_contents($this->path()));

                if (null === $data) {
                    var_dump($this->path(), file_get_contents($this->path()));
                    throw new Exception('boom 2');
                }

                foreach ($data->records ?? [] as $record) {
                    $table = $record->table;
                    $id = $record->id;

                    $this->records[$id . '.' . $table] = $record;
                }

                $this->links = (array) ($data->links ?? []);

                $this->previous = $data->previous;
                $this->height = $data->height;
                $this->timestamp = $data->timestamp;
                $this->meta = (array) ($data->meta ?? []);
            }

            $this->next = @file_get_contents($this->lockFile()) ?: null;
            $this->loaded = true;
        }

        return $this;
    }

    public function chain(): Chain
    {
        return $this->chain;
    }

    public function mkdir(): self
    {
        Helper::mkdir(dirname($this->path()), $this->chain->basePath());

        return $this;
    }

    public function recordExists(string $table, string $id): bool
    {
        if (isset($this->records[$id . '.' . $table])) {
            return true;
        }

        return Record::of($this, $table, $id)->exists();
    }

    public function save(): self
    {
        $this->mkdir();

        $data = array_filter([
            'height' => $this->height,
            'links' => $this->links,
            'meta' => $this->meta,
            'previous' => $this->previous,
            'records' => array_values($this->records),
            'timestamp' => $this->timestamp,
        ]);

        $json = json_encode($data, Jars::ENCODING_OPTIONS);

        if (!$json) {
            array_walk($this->records, function ($r) { if (isset($r->content)) $r->content = 'something'; });
            var_dump($this->path(), $json, $this->records);
            throw new Exception('boom 10');
        }

        file_put_contents($this->path(), $json);

        return $this;
    }

    public function height(?int $new = null): int|self
    {
        if (func_num_args()) {
            $this->height = $new;

            return $this;
        }

        if ($this->version === Jars::INITIAL_VERSION) {
            return 0;
        }

        return $this->height;
    }

    public function addMeta(string $new): self
    {
        $this->meta[] = $new;

        return $this;
    }

    public function meta(?array $new = null): array|self
    {
        if (func_num_args()) {
            $this->meta = $new;

            return $this;
        }

        if ($this->version === Jars::INITIAL_VERSION) {
            return [];
        }

        return $this->meta;
    }

    public function lockFile(): string
    {
        return $this->path() . '.next';
    }

    public function advance(): ?self
    {
        if (null === $next = $this->next()) {
            return null;
        }

        return $this->chain->getBlock($next);
    }

    public function timestamp(string $new): string|self
    {
        if (func_num_args()) {
            $this->timestamp = $new;

            return $this;
        }

        return $this->timestamp;
    }

    public function preLock(): void
    {
        if (Jars::INITIAL_VERSION === $this->version) {
            $this->mkdir();
        }
    }

    public function takeANumber(): string
    {
        $id = hash('sha256', hex2bin(hash('sha256', $this->pointer . '--' . $this->version)));

        $this->chain->trigger('takeanumber', $this->pointer, $id);

        $this->pointer++;

        return $id;
    }

    public function pointer(): int
    {
        if ($this->version === Jars::INITIAL_VERSION) {
            return 0;
        }

        return $this->pointer;
    }
}