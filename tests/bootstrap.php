<?php

declare(strict_types=1);

require dirname(__DIR__).'/vendor/autoload.php';

date_default_timezone_set('UTC');

/**
 * Writable directory for test kernel caches, logs and sqlite files.
 */
define('MWF_TEST_VAR_DIR', sys_get_temp_dir().'/mwf-bundle-tests');

if (!is_dir(MWF_TEST_VAR_DIR)) {
    mkdir(MWF_TEST_VAR_DIR, 0777, true);
}
