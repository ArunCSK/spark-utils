from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit
class SchemaUtils:
    """
    Utility class for schema handling and slow-changing dimensions.
    """

    @staticmethod
    def merge_schemas(source_df: DataFrame, target_df: DataFrame):
        if target_df is not None and len(source_df.schema.names) != len(target_df.schema.names):
            source_schema = set(source_df.schema.names)
            target_schema = set(target_df.schema.names)

            missing_in_target = source_schema - target_schema
            missing_in_source = target_schema - source_schema

            for col in missing_in_target:
                target_df = target_df.withColumn(col, lit(None))

            for col in missing_in_source:
                source_df = source_df.withColumn(col, lit(None))

            return source_df.select(target_df.schema.names), target_df.select(target_df.schema.names)
        return source_df, target_df
