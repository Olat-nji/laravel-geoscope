<?php

namespace Netsells\GeoScope\ScopeDrivers;

use Illuminate\Support\Facades\DB;
final class PostgreSQLScopeDriver extends AbstractScopeDriver
{
    /**
     * @param float $lat
     * @param float $long
     * @param float $distance
     * @return mixed
     */
    public function withinDistanceOf(float $lat, float $long, float $distance)
    {
        return $this->query->whereRaw($this->getWithinDistanceSQL(), [
            $lat,
            $long,
            $distance,
        ]);
    }

    /**
     * @param float $lat
     * @param float $long
     * @param float $distance
     * @return mixed
     */
    public function orWithinDistanceOf(float $lat, float $long, float $distance)
    {
        return $this->query->orWhereRaw($this->getWithinDistanceSQL(), [
            $lat,
            $long,
            $distance,
        ]);
    }

    /**
     * @throws InvalidOrderDirectionParameter
     * @param float $lat
     * @param float $long
     * @param float $orderDirection
     * @return mixed
     */
    public function orderByDistanceFrom(float $lat, float $long, string $orderDirection = 'asc')
    {
        $this->checkOrderDirectionIdentifier($orderDirection);

        // Generate the raw SQL string for ordering by distance
        if (version_compare(app()->version(), '10.0', '>')) {
            $orderByDistanceSQL = DB::raw($this->getOrderByDistanceSQL($orderDirection))
                ->getValue(DB::connection()->getQueryGrammar());
        } else {
            // Use the raw SQL string directly for older Laravel versions
            $orderByDistanceSQL = DB::raw($this->getOrderByDistanceSQL($orderDirection));
        }

        // Apply the ordering to the query
        return $this->query->orderByRaw($orderByDistanceSQL, [
            $long,
            $lat,
        ]);
    }

    /**
     * @param float $lat
     * @param float $long
     * @param string $fieldName
     * @return mixed
     */
    public function addDistanceFromField(float $lat, float $long, ?string $fieldName = null)
    {
        $fieldName = $this->getValidFieldName($fieldName);
       
        $this->query->select('*');

        return $this->query->selectRaw($this->getSelectDistanceSQL($fieldName), [
            $lat,
            $long,
        ])->selectRaw("'{$this->config['units']}' as {$fieldName}_units");
    }

    /**
     * @return string
     */
    private function getWithinDistanceSQL(): string
    {
        return <<<EOD
            earth_distance(
                ll_to_earth({$this->config['lat-column']}, {$this->config['long-column']}),
                ll_to_earth(?, ?)
             ) * {$this->conversion} < ?
EOD;
    }

    /**
     * @return string
     */
    private function getOrderByDistanceSQL(string $orderDirection): string
    {
        return <<<EOD
            earth_distance(
                ll_to_earth({$this->config['lat-column']}, {$this->config['long-column']}),
                ll_to_earth(?, ?)
             ) * {$this->conversion} {$orderDirection}
EOD;
    }

    /**
     * @return string
     */
    private function getSelectDistanceSQL(string $fieldName): string
    {
        return <<<EOD
            earth_distance(
                ll_to_earth({$this->config['lat-column']}, {$this->config['long-column']}),
                ll_to_earth(?, ?)
             ) * {$this->conversion} as {$fieldName}
EOD;
    }
}
