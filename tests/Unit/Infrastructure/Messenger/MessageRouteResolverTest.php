<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Unit\Infrastructure\Messenger;

use Contracts\Demo\Command\DoSomethingCommand;
use Kraz\MessengerWorkflow\Application\Attribute\MessageRoute;
use Kraz\MessengerWorkflow\Infrastructure\Messenger\MessageRouteResolver;
use Kraz\MessengerWorkflow\Tests\Fixture\Message\Routing\AmbiguousRoutedCommand;
use Kraz\MessengerWorkflow\Tests\Fixture\Message\Routing\AttributeInheritingCommand;
use Kraz\MessengerWorkflow\Tests\Fixture\Message\Routing\AttributeRoutedCommand;
use Kraz\MessengerWorkflow\Tests\Fixture\Message\Routing\AttributeRoutedQuery;
use Kraz\MessengerWorkflow\Tests\Fixture\Message\Routing\ConcreteRoutedCommand;
use Kraz\MessengerWorkflow\Tests\Fixture\Message\Routing\InterfaceRoutedCommand;
use Kraz\MessengerWorkflow\Tests\Fixture\Message\Routing\OtherRoutedCommandInterface;
use Kraz\MessengerWorkflow\Tests\Fixture\Message\Routing\RoutedCommandInterface;
use PHPUnit\Framework\TestCase;

/**
 * Spec: routes are resolved from the compiled configuration map first, then from the
 * #[MessageRoute] attribute — each along the class lineage (class, parents, interfaces).
 */
final class MessageRouteResolverTest extends TestCase
{
    public function testAnUnroutedClassResolvesToNull(): void
    {
        self::assertNull(new MessageRouteResolver([])->resolve(new DoSomethingCommand()));
    }

    public function testAConfiguredClassResolvesToItsRoute(): void
    {
        $resolver = new MessageRouteResolver([DoSomethingCommand::class => 'planning']);

        self::assertSame('planning', $resolver->resolve(new DoSomethingCommand()));
        self::assertSame('planning', $resolver->resolve(DoSomethingCommand::class));
    }

    public function testAConfiguredParentClassRoutesItsSubclasses(): void
    {
        $resolver = new MessageRouteResolver([\Kraz\MessengerWorkflow\Tests\Fixture\Message\Routing\AbstractRoutedCommand::class => 'bulk']);

        self::assertSame('bulk', $resolver->resolve(new ConcreteRoutedCommand()));
    }

    public function testAConfiguredInterfaceRoutesItsImplementors(): void
    {
        $resolver = new MessageRouteResolver([RoutedCommandInterface::class => 'bulk']);

        self::assertSame('bulk', $resolver->resolve(new InterfaceRoutedCommand()));
    }

    public function testInterfacesNamingDifferentRoutesAreAmbiguous(): void
    {
        $resolver = new MessageRouteResolver([RoutedCommandInterface::class => 'a', OtherRoutedCommandInterface::class => 'b']);

        $this->expectException(\LogicException::class);
        $this->expectExceptionMessageMatches('/ambiguous/');

        $resolver->resolve(new AmbiguousRoutedCommand());
    }

    public function testInterfacesNamingTheSameRouteAreNotAmbiguous(): void
    {
        $resolver = new MessageRouteResolver([RoutedCommandInterface::class => 'a', OtherRoutedCommandInterface::class => 'a']);

        self::assertSame('a', $resolver->resolve(new AmbiguousRoutedCommand()));
    }

    public function testTheAttributeRoutesAClassAndItsSubclasses(): void
    {
        $resolver = new MessageRouteResolver([]);

        self::assertSame('slow', $resolver->resolve(new AttributeRoutedCommand()));
        self::assertSame('slow', $resolver->resolve(new AttributeInheritingCommand()));
        self::assertSame('reports', $resolver->resolve(new AttributeRoutedQuery()));
    }

    public function testTheConfigurationWinsOverTheAttribute(): void
    {
        $resolver = new MessageRouteResolver([AttributeRoutedCommand::class => 'configured']);

        self::assertSame('configured', $resolver->resolve(new AttributeRoutedCommand()));
    }

    public function testAnUnknownClassNameResolvesToNull(): void
    {
        self::assertNull(new MessageRouteResolver(['Nope\Missing' => 'x'])->resolve('Nope\Missing'));
    }

    public function testTheAttributeRejectsInvalidRouteNames(): void
    {
        $this->expectException(\InvalidArgumentException::class);

        new MessageRoute('two.segments');
    }
}
