<?php

namespace Netsells\GeoScope\Tests\Integration\ScopeDrivers;

use Illuminate\Database\Eloquent\Builder;

class DefaultScopeDriverTest extends ScopeDriverTestCase
{
    protected $scopeDriver;

    public function setUp(): void
    {
        parent::setUp();
        $this->scopeDriver = $this->getScopeDriver('default');
    }

    /**
     * @test
     */
    public function within_distance_of_returns_query_correct_builder()
    {
        $actual = $this->scopeDriver->withinDistanceOf(54.7742, -1.33919, 20);

        $this->assertSame($actual, $this->query);
        $this->assertEquals(get_class($actual), Builder::class);
    }

    /**
     * @test
     */
    public function or_within_distance_of_returns_query_correct_builder()
    {
        $actual = $this->scopeDriver->withinDistanceOf(54.7742, -1.33919, 20);

        $this->assertSame($actual, $this->query);
        $this->assertEquals(get_class($actual), Builder::class);
    }

    /**
     * @test
     */
    public function within_distance_of_applied_correct_sql()
    {
        $lat = 54.60653;
        $long = -3.347538;
        $dist = 20;

        $query = $this->scopeDriver->withinDistanceOf($lat, $long, $dist);
        $bindings = $this->query->getBindings();

        $this->assertStringContainsString("sin(radians(54.60653))", $query->toSql());
        $this->assertStringContainsString(' where ', $query->toSql());

        $this->assertCount(1, $bindings);

        $this->assertTrue(in_array($dist, $bindings));
    }

    /**
     * @test
     */
    public function or_within_distance_of_applied_correct_sql()
    {
        $lat = 54.60653;
        $long = -3.347538;
        $dist = 20;

        $this->query->where('id', 1);
        $query = $this->scopeDriver->orWithinDistanceOf($lat, $long, $dist);

        $bindings = $this->query->getBindings();

        $this->assertStringContainsString("sin(radians(54.60653)) ", $query->toSql());
        $this->assertStringContainsString(' or ', $query->toSql());

        $this->assertCount(2, $bindings);

        $this->assertTrue(in_array($dist, $bindings));
        $this->assertTrue(in_array(1, $bindings));
    }
}
