<?php

namespace jars\events;

interface entryimported extends \jars\Listener
{
    public function handle_entryimported();
}
