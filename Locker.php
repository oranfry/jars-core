<?php

namespace OranFry\Jars\Core;

use OranFry\Jars\Contract\Exception;

class Locker
{
    private array $locks = [];
    private string $reportsLockFile;
    private string $touchFile;
    private string $head;

    public function __construct(string $touchFile, string $reportsLockFile)
    {
        $this->touchFile = $touchFile;
        $this->reportsLockFile = $reportsLockFile;
    }

    public function head(): int
    {
        return $this->head;
    }

    private function isLocked(string $file): bool
    {
        return isset($this->locks[$file]);
    }

    public function isPrimaryLocked(): bool
    {
        return $this->isLocked($this->touchFile);
    }

    public function isReportsLocked(): bool
    {
        return $this->isLocked($this->reportsLockFile);
    }

    private function lock(string $file, ?string $contents = null): ?object
    {
        if (isset($this->locks[$file])) {
            return null; // locked by a higher-level import
        }

        @mkdir(dirname($file), 0777, true);

        $lock = (object) [];

        // TODO: Implement timeout using non-blocking locking in a loop until

        if (!($lock->handle = fopen($file, 'c+'))) {
            throw new Exception('Could not open the touch file');
        }

        if (!flock($lock->handle, LOCK_EX)) {
            fclose($lock->handle);

            throw new Exception('Could not acquire a lock over the touch file');
        }

        $contents = fgets($lock->handle);

        $lock->pin = bin2hex(random_bytes(32));

        $this->locks[$file] = $lock;

        return (object) [
            'pin' => $lock->pin,
            'contents' => $contents,
        ];
    }

    public function lockPrimary(): ?string
    {
        if (null === $result = $this->lock($this->touchFile)) {
            return null;
        }

        $this->head = intval(trim($result->contents) ?: '0');

        return $result->pin;
    }

    public function lockReports(): ?string
    {
        if (null === $result = $this->lock($this->reportsLockFile)) {
            return null;
        }

        return $result->pin;
    }

    private function unlock(string $file, ?string $pin, ?string $contents = null): self
    {
        if (!isset($this->locks[$file])) {
            throw new Exception('Attempt to unlock when not locked [' . $file . ']');
        }

        $lock = $this->locks[$file];

        if ($lock->pin !== $pin) {
            throw new Exception('Incorrect locker PIN provided for unlocking');
        }

        if (null !== $contents) {
            ftruncate($lock->handle, 0);
            fwrite($lock->handle, $contents);
            fsync($lock->handle);
        }

        fclose($lock->handle);

        unset($this->locks[$file]);

        return $this;
    }

    public function unlockPrimary(string $pin, ?string $head = null): self
    {
        return $this->unlock($this->touchFile, $pin, $head);
    }

    public function unlockReports(string $pin): self
    {
        return $this->unlock($this->reportsLockFile, $pin);
    }
}
