from dagster_quickstart.io_manager import duckdb_parquet_io_manager, DuckDBParquetTypeHandler
import dagster as dg

@dg.definitions
def resources():
    return dg.Definitions(
        resources={
            "io_manager": duckdb_parquet_io_manager.configured({"database": ":memory:"})
        }
    )