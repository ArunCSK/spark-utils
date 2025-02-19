from pyspark.sql import SparkSession, DataFrame
from delta.tables import DeltaTable

class FileWriter:
    """
    Utility class to write DataFrames to different file formats.
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def write_file(self, df: DataFrame, file_path: str, file_format: str, mode: str = "overwrite",
                   partition_by: list = None):
        file_format = file_format.lower()

        if file_format == "csv":
            df.write.mode(mode).csv(file_path, header=True)
        elif file_format == "excel":
            pandas_df = df.toPandas()
            pandas_df.to_excel(file_path, index=False, engine='openpyxl')
        elif file_format == "parquet":
            df.write.mode(mode).parquet(file_path)
        elif file_format == "delta":
            if partition_by:
                df.write.mode(mode).partitionBy(partition_by).format("delta").save(file_path)
            else:
                df.write.mode(mode).format("delta").save(file_path)
        else:
            raise ValueError(f"Unsupported file format: {file_format}")

    def upsert_delta(self, df: DataFrame, delta_path: str, primary_key: str):


        if DeltaTable.isDeltaTable(self.spark, delta_path):
            delta_table = DeltaTable.forPath(self.spark, delta_path)
            delta_table.alias("target").merge(
                df.alias("source"), f"target.{primary_key} = source.{primary_key}"
            ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        else:
            df.write.format("delta").mode("overwrite").save(delta_path)