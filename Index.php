<?php

namespace OranFry\Jars\Core;

use OranFry\Jars\Contract\Exception;
use OranFry\Jars\Contract\StaleLockException;
use OranFry\Jars\Contract\Constants;

class Index
{
    use Lockable;

    private string $basePath;
    private Jars $jars;

    private ?string $head = null;
    private ?string $height = null;
    private ?string $locker_pin = null;

    private array $recordVersions = [];
    private array $linkVersions = [];

    public function __construct(Jars $jars, string $basePath)
    {
        $this->jars = $jars;
        $this->basePath = $basePath;
    }

    public function dataFile(string $id, ...$suffixes): string
    {
        return $this->jars->dataFile($this->basePath, $id, ...$suffixes);
    }

    public function head(?string $new = null): string|self
    {
        if (func_num_args()) {
            $this->head = $new;

            return $this;
        }

        if ($this->head !== null) {
            return $this->head;
        }

        $file = $this->basePath . '/head';

        return is_file($file) ? file_get_contents($file) : Constants::ROOT_VERSION;
    }

    public function height(): int
    {
        if ($this->height !== null) {
            return $this->height;
        }

        if (!is_file($file = $this->basePath . '/height')) {
            return 0;
        }

        return $this->height = (int) file_get_contents($file);
    }

    public function recordVersion(string $id, ?string $version = null): null|string|self
    {
        if (func_num_args() > 1) {
            $this->recordVersions[$id] = $version;

            return $this;
        }

        if (!array_key_exists($id, $this->recordVersions)) {
            $this->recordVersions[$id] = $version = is_file($file = $this->dataFile($id)) ? file_get_contents($file) : null;
        }

        return $this->recordVersions[$id];
    }

    public function linkVersion(string $linkName, string $recordId, bool $reverse, ?string $version = null): null|string|self
    {
        $key = Block::linkKey($linkName, $recordId, $reverse);

        if (func_num_args() > 3) {
            $this->linkVersions[$key] = $version;

            return $this;
        }

        if (!array_key_exists($key, $this->linkVersions)) {
            $file = $this->dataFile($recordId, $linkName, $reverse ? 'back' : 'forth');

            if (!is_file($file)) {
                return null;
            }

            $this->linkVersions[$key] = file_get_contents($file);
        }

        return $this->linkVersions[$key];
    }

    function addToChain($newBlock, bool $permanent = true): self
    {
        if (
            $newBlock->height() === $this->height()
            || $newBlock->version() === $this->head
        ) {
            // the work is already done, perhaps by index rebuild
            return $this;
        }

        if ($newBlock->height() !== $this->height() + 1) {
            throw new Exception('Incorrect height of block being added to index');
        }

        $this->head = $newBlock->version();
        $this->height = $newBlock->height();

        foreach ($newBlock->recordIds() as $id) {
            if ($permanent) {
                $file = $this->dataFile($id);

                Helper::mkdir(dirname($file), $this->basePath);
                file_put_contents($file, $this->head);
            }

            $this->recordVersions[$id] = $this->head;
        }

        foreach ($newBlock->linkKeys() as $key) {
            if ($permanent) {
                $file = $this->dataFile(...explode('.', $key));

                Helper::mkdir(dirname($file), $this->basePath);
                file_put_contents($file, $this->head);
            }

            $this->linkVersions[$key] = $this->head;
        }

        if ($permanent) {
            file_put_contents($this->basePath . '/head', $this->head);
            file_put_contents($this->basePath . '/height', $this->height);
        }

        if (defined('JARS_VERBOSE') && JARS_VERBOSE) {
            error_log("Added block to index [$this->head] [$this->height]");
        }

        return $this;
    }

    public function lockFile(): string
    {
        return $this->basePath . '.lock';
    }

    public function postUnlock(): void
    {
        // unlike a block, we need to be able to lock the index time and time again
        // remove the lock file so fopen can succeed next time around

        if (defined('JARS_VERBOSE') && JARS_VERBOSE) {
            $file = $this->lockFile();
            error_log("Removing [$file]");
        }

        unlink($this->lockFile());
    }

    public function nuke(): self
    {
        $lockFile = $this->lockFile();

        `rm -f "$lockFile"`;

        $this->lock(600);

        `rm -rf "$this->basePath"`;

        mkdir($this->basePath);

        return $this;
    }

    public function safeLock(int $timeout = 10): self
    {
        try {
            $this->lock($timeout);
        } catch (StaleLockException $sle) {
            if (defined('JARS_VERBOSE') && JARS_VERBOSE) {
                error_log('Safe Lock: Handling error: ' . $sle->getMessage());
            }

            $this->jars->rebuildIndex();
        }

        return $this;
    }

    public function revert(): self
    {
        $this->head = null;
        $this->height = null;
        $this->locker_pin = null;
        $this->recordVersions = [];
        $this->linkVersions = [];

        return $this;
    }
}
