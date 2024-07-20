from utils.common import CommonUtils
from utils.confutils import CONF

class DeltaFullLoad(CONF):

    def __init__(self) -> None:
        self.utils = CommonUtils()
        self.table_name = "customer"
        self.schema_name = "master"
        self.source_path = f"s3a://{self.s3_bucket}/landing/{self.table_name}"
        self.destination_path = f"s3a://{self.s3_bucket}/{self.schema_name}/{self.table_name}""
        self.catalog_name = "delta_ops"
        self.destination_table_full_name = f"{self.catalog_name}.{self.schema_name}.{self.table_name}"
        self.write_mode = "overwrite" # Full Load


    def write_delta_full_load_logic(self):
        df = self.utils.get_df(self.source_path, "parquet", True)

        if self.utils.delta_exits(table_name=self.destination_table_full_name):
            self.utils.delta_write_without_partition(df, self.destination_path, self.destination_table_full_name, self.write_mode)

if __name__ == '__main__':
    DeltaFullLoad().write_delta_full_load_logic()

