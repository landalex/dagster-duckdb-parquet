from collections.abc import Sequence
from typing import Optional

from dagster import EnvVar, InputContext, MetadataValue, OutputContext, TableColumn, TableSchema, get_dagster_logger
from dagster._core.definitions.metadata import TableMetadataSet
from dagster._core.storage.db_io_manager import DbTypeHandler, TableSlice
from dagster_duckdb import DuckDBIOManager, build_duckdb_io_manager
from string import Template
from sqlescapy import sqlescape

class SQL:
    def __init__(self, sql, **bindings):
        self.sql = sql
        self.bindings = bindings


def sql_to_string(s: SQL) -> str:
    replacements = {}
    for key, value in s.bindings.items():
        if isinstance(value, SQL):
            replacements[key] = f"({sql_to_string(value)})"
        elif isinstance(value, str):
            replacements[key] = f"'{sqlescape(value)}'"
        elif isinstance(value, (int, float, bool)):
            replacements[key] = str(value)
        elif value is None:
            replacements[key] = "null"
        else:
            raise ValueError(f"Invalid type for {key}")
    return Template(s.sql).safe_substitute(replacements)


class DuckDBParquetTypeHandler(DbTypeHandler[SQL]):
    def _get_identifier(self, context):
        if context.has_asset_key:
            return context.get_asset_identifier()
        else:
            return context.get_identifier()
        
    def _get_table_name(self, context):
        asset_key = self._get_identifier(context)
        if context.has_asset_partitions:
            asset_key = asset_key[:-1]
        
        return '_'.join(asset_key)

    def _get_s3_url(self, context):
        bucket_name = EnvVar('S3_BUCKET_NAME').get_value()
        prefix = EnvVar('S3_PREFIX').get_value()
        return f"src/dagster_quickstart/out/{bucket_name}/{prefix}{'/'.join(self._get_identifier(context))}"

    def handle_output(self, context: OutputContext, table_slice: TableSlice, select_statement: SQL, connection):
        if select_statement is None:
            return

        if not isinstance(select_statement, SQL):
            raise ValueError(
                f"Expected asset to return a SQL; got {select_statement!r}"
            )
        
        logger = get_dagster_logger()

        statement_str = sql_to_string(
                SQL(
                    "CREATE OR REPLACE TABLE $table AS $select_statement",
                    table=self._get_table_name(context),
                    select_statement=select_statement
                )
            )

        logger.info(f"Executing {statement_str}")

        connection.execute(
            statement_str
        )

        connection.execute(
            sql_to_string(
                SQL(
                    "COPY (SELECT * FROM $table) TO $url (FORMAT parquet, COMPRESSION zstd, FILE_SIZE_BYTES '256m', OVERWRITE true)",
                    table=self._get_table_name(context),
                    url=self._get_s3_url(context),
                )
            )
        )

        row_count = connection.execute(
            sql_to_string(
                SQL(
                    "SELECT count(*) FROM $table",
                    table=self._get_table_name(context)
                )
            )
        ).fetchone()

        describe_result = connection.execute(
            sql_to_string(
                SQL(
                    "DESCRIBE $table",
                    table=self._get_table_name(context)
                )
            )
        ).fetchall()

        context.add_output_metadata(
            {
                # output object may be a slice/partition, so we output different metadata keys based on
                # whether this output represents an entire table or just a slice/partition
                **(
                    TableMetadataSet(partition_row_count=row_count[0])
                    if context.has_partition_key
                    else TableMetadataSet(row_count=row_count[0])
                ),
                "dagster/column_schema": MetadataValue.table_schema(
                    TableSchema(
                        columns=[
                            TableColumn(
                                name = col[0], type = col[1]
                            ) 
                            for col in describe_result
                        ]
                    )
                )
            }
        )

    def load_input(self, context: InputContext, table_slice: TableSlice, connection) -> SQL:
        return SQL("SELECT * FROM read_parquet($url)", url=f"{self._get_s3_url(context)}/*")
    
    @property
    def supported_types(self):
        return [SQL]

duckdb_parquet_io_manager = build_duckdb_io_manager(
    [DuckDBParquetTypeHandler()], default_load_type=SQL
)
    
class DuckDBParquetIOManager(DuckDBIOManager):
    @classmethod
    def _is_dagster_maintained(cls) -> bool:
        return True

    @staticmethod
    def type_handlers() -> Sequence[DbTypeHandler]:
        return [DuckDBParquetTypeHandler()]

    @staticmethod
    def default_load_type() -> Optional[type]:
        return SQL