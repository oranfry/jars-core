<?php

namespace OranFry\Jars\Core\Events;

interface importline extends \OranFry\Jars\Core\Listener
{
    public function handle_importline(string $table);
}
