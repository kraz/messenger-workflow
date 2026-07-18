<?php

declare(strict_types=1);

require dirname(__DIR__).'/vendor/autoload.php';

date_default_timezone_set('UTC');

// Isolated writable dir for kernel cache / sqlite databases
$varDir = sys_get_temp_dir().'/mwf-bundle-tests';
if (!is_dir($varDir)) {
    mkdir($varDir, 0777, true);
}
define('MWF_TEST_VAR_DIR', $varDir);
