from pyspark.sql import SparkSession
from delta import DeltaTable

class CommonUtils():

    def __init__(self):
        self.spark = self.__create_spark_session__("Spark_Session_Init")

    def __create_spark_session__(self, session_name):
        return SparkSession.Builder \
                .getOrCreate(session_name)
    
    def get_df(self, path, format, isheader):
        return self.spark.read \
                    .format(format) \
                    .option("header", isheader) \
                    .load(path)
    
    def delta_exits(self, path=None, table_name=None):
        if path is not None and DeltaTable.isDeltaTable(path):
            return True
        elif table_name is not None and self.spark.catalog.tableExists(table_name):
            return True
        else:
            return False

    def delta_write_without_partition(self, df, path, table_name, write_mode):
        try:
            df.write \
                .format("delta") \
                .option("path", path) \
                .mode(write_mode) \
                .saveAsTable(table_name)
        except Exception:
            pass
