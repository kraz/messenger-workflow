<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Messenger\Doctrine;

use Doctrine\DBAL\Connection as DBALConnection;

final class DbalConnectionComparator
{
    /**
     * Connection params that do not influence which database is being talked to.
     */
    private const array IGNORED_PARAMS = ['wrapperClass', 'driverOptions', 'defaultTableOptions', 'charset', 'serverVersion'];

    /**
     * Whether both connections point to the same database. The comparison covers the
     * union of both param sets — a distinguishing param present on only one side (e.g.
     * "memory" vs "path" for sqlite) counts as a difference. Unknown differences make
     * the check fail, i.e. it errs on the side of "not the same".
     */
    public static function isSameDatabase(DBALConnection $connectionA, DBALConnection $connectionB): bool
    {
        if ($connectionA === $connectionB) {
            return true;
        }

        return self::normalizeParams($connectionA->getParams()) === self::normalizeParams($connectionB->getParams());
    }

    private static function normalizeParams(array $params): array
    {
        $params = array_diff_key($params, array_flip(self::IGNORED_PARAMS));
        $params = array_filter($params, static fn ($value) => \is_scalar($value));
        $params = array_map(static fn ($value) => \is_string($value) ? strtolower($value) : $value, $params);
        ksort($params);

        return $params;
    }
}
