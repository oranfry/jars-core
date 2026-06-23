<?php

namespace OranFry\Jars\Core\Events;

interface entryimported extends \OranFry\Jars\Core\Listener
{
    public function handle_entryimported();
}
