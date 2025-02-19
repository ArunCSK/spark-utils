from pyspark.sql import SparkSession, DataFrame
import pandas as pd


class FileReader:
    """
    Utility class to read various file formats into a Spark DataFrame.
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def read_file(self, file_path: str, file_format: str, options: dict = {}):
        file_format = file_format.lower()

        if file_format == "csv":
            return self.spark.read.options(**options).csv(file_path, header=True, inferSchema=True)
        elif file_format == "excel":
            pandas_df = pd.read_excel(file_path, engine='openpyxl')
            return self.spark.createDataFrame(pandas_df)
        elif file_format == "parquet":
            return self.spark.read.parquet(file_path)
        elif file_format == "delta":
            return self.spark.read.format("delta").load(file_path)
        else:
            raise ValueError(f"Unsupported file format: {file_format}")





