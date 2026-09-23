<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Application\Attribute;

use Kraz\MessengerWorkflow\Infrastructure\Messenger\RoutingKey;

/**
 * Assigns a command or query class to a named route of its bounded context. A routed
 * message is published with the routing key `<broker>.[internal.]<Context>.<route>`
 * and therefore reaches only the queue declared with `route: <route>` for that context
 * (`messenger_workflow.messenger.transports.<broker>.queue_bindings`) — never the
 * context's regular queue.
 *
 * The attribute is the code-side equivalent of listing the class under the binding's
 * `messages`; subclasses and implementors inherit it. A class listed in the
 * configuration under one route while carrying the attribute for another fails the
 * container build.
 */
#[\Attribute(\Attribute::TARGET_CLASS)]
final readonly class MessageRoute
{
    public function __construct(public string $route)
    {
        if (1 !== preg_match(RoutingKey::ROUTE_PATTERN, $route)) {
            throw new \InvalidArgumentException(\sprintf('Invalid message route "%s": a route is one routing-key segment (letters, digits and underscores).', $route));
        }
    }
}
