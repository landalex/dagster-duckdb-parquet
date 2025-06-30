import dagster as dg
from dagster_quickstart.io_manager import SQL


@dg.asset()
def air_quality_raw() -> SQL:
    return SQL(
        """
        SELECT * FROM 'src/dagster_quickstart/defs/data/*.parquet'
        """
    )
        

@dg.asset()
def pollutant_index() -> SQL:
    return SQL(
        """
        SELECT 
            *,
            CAST(SUBSTRING(URI, LENGTH(URI) - POSITION('/' IN REVERSE(URI)) + 2) AS INTEGER) AS pollutant_id
        FROM 'src/dagster_quickstart/defs/data/pollutant.csv'
        """
    )

        
@dg.asset()
def air_quality_co(air_quality_raw: SQL, pollutant_index: SQL) -> SQL:
    return SQL(
        """
        SELECT
            pi.Notation as notation,
            pi.pollutant_id as pollutant_id,
            aq.Start as start,
            aq.End as end,
            aq.Value as value,
            aq.Unit as unit,
            aq.AggType as agg_type
            FROM $air_quality_raw AS aq
            JOIN $pollutant_index AS pi
            ON aq.Pollutant = pi.pollutant_id
            WHERE Notation = 'CO'
        """,
        air_quality_raw=air_quality_raw,
        pollutant_index=pollutant_index
    )


@dg.asset()
def air_quality_pm(air_quality_raw: SQL, pollutant_index: SQL) -> SQL:
    return SQL(
        """
        SELECT
            pi.Notation as notation,
            pi.pollutant_id as pollutant_id,
            aq.Start as start,
            aq.End as end,
            aq.Value as value,
            aq.Unit as unit,
            aq.AggType as agg_type
            FROM $air_quality_raw AS aq
            JOIN $pollutant_index AS pi
            ON aq.Pollutant = pi.pollutant_id
            WHERE 
                Notation = 'PM2.5' OR
                Notation = 'PM1'
        """,
        air_quality_raw=air_quality_raw,
        pollutant_index=pollutant_index
    )