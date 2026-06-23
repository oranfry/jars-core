<?php

namespace OranFry\Jars\Core\Events;

interface takeanumber extends \OranFry\Jars\Core\Listener
{
    public function handle_takeanumber(int $pointer, string $id);
}
