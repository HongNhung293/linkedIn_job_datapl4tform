# from typing import Union
# import pandas as pd
# from dagster import IOManager, OutputContext, InputContext
# from contextlib import contextmanager
# import clickhouse_connect

# # CLICKHOUSE_CONFIG = {
# #     "host": "clickhouse",        
# #     "port": 8123,                
# #     "username": "admin",
# #     "password": "admin123",
# #     "database": "warehouse",
# # }


# @contextmanager
# def connect_to_clickhouse(config):

#     client = clickhouse_connect.get_client(
#         host=config.get("host"),
#         port=config.get("port", 8123),
#         username=config.get("username"),
#         password=config.get("password"),
#         database=config.get("database"),
#     )
#     try:
#         yield client
#     finally:
#         client.close()


# class ClickHouseIOManager(IOManager):
#     def __init__(self, config):
#         self.config = config

#     def _get_table_name(self, context: Union[InputContext, OutputContext]) -> str:
#         """
#         layer/schema/folder/table -> schema.table
#         """
#         layer, schema, folder, table = context.asset_key.path
#         return f"warehouse.{table}"

#     def handle_output(self, context: OutputContext, obj: pd.DataFrame):
#         table_name = self._get_table_name(context)

#         if obj is None or obj.empty:
#             context.log.warning(f"No data to insert into {table_name}")
#             return

#         with connect_to_clickhouse(self.config) as client:
#             db_name = self.config.get("database")

#             context.log.info(obj.dtypes)

#             client.insert_df(table_name, obj)

#             context.log.info(f"Inserted {len(obj)} records into {table_name}")
#             context.add_output_metadata({
#                 "table": table_name,
#                 "records": len(obj),
#             })


#     def load_input(self, context: InputContext) -> pd.DataFrame:
#         table_name = self._get_table_name(context)
#         with connect_to_clickhouse(self.config) as client:
#             query = f"SELECT * FROM {table_name}"
#             df = client.query_df(query)
#         return df


import pandas as pd
from dagster import IOManager, OutputContext, InputContext
from contextlib import contextmanager
import clickhouse_connect


@contextmanager
def connect_to_clickhouse(config):
    client = clickhouse_connect.get_client(
        host=config.get("host"),
        port=config.get("port", 8123),
        username=config.get("username"),
        password=config.get("password"),
        database=config.get("database"),
    )
    try:
        yield client
    finally:
        client.close()


class ClickHouseIOManager(IOManager):
    def __init__(self, config):
        self.config = config

    def _get_table_name(self, context) -> str:
        layer, schema, folder, table = context.asset_key.path
        return f"warehouse.{table}"

    def _ensure_table(self, client, table_name: str, df: pd.DataFrame, context: OutputContext):
        """Tạo bảng nếu chưa có"""
        exists = client.command(f"EXISTS TABLE {table_name}")
        if exists == 1:
            return

        # Tạo schema từ DataFrame
        cols = []
        for col, dtype in df.dtypes.items():
            if pd.api.types.is_integer_dtype(dtype):
                ch_type = "Int64"
            elif pd.api.types.is_float_dtype(dtype):
                ch_type = "Float64"
            elif pd.api.types.is_bool_dtype(dtype):
                ch_type = "UInt8"
            else:
                ch_type = "String"
            cols.append(f"`{col}` {ch_type}")
        schema_sql = ", ".join(cols)

        create_sql = f"""
        CREATE TABLE {table_name} (
            {schema_sql}
        ) ENGINE = MergeTree()
        ORDER BY tuple()
        """
        context.log.info(f"Creating table: {create_sql}")
        client.command(create_sql)

    def handle_output(self, context: OutputContext, df: pd.DataFrame):
        table_name = self._get_table_name(context)
        if df is None or df.empty:
            context.log.warning(f"No data to insert into {table_name}")
            return

        with connect_to_clickhouse(self.config) as client:
            self._ensure_table(client, table_name, df, context)

            # Lấy schema ClickHouse
            schema = client.query_df(f"DESCRIBE TABLE {table_name}")
            ch_cols = schema["name"].tolist()

            # Chuẩn hóa DataFrame theo schema ClickHouse
            for col in ch_cols:
                if col not in df.columns:
                    df[col] = None  # Thêm cột missing
                ch_type = schema.loc[schema["name"] == col, "type"].values[0]

                if "UInt" in ch_type or "Int" in ch_type:
                    if "Nullable" in ch_type:
                        df[col] = df[col].astype("Int64")
                    else:
                        df[col] = df[col].fillna(0).astype("UInt64" if "UInt" in ch_type else "Int64")
                elif "Float" in ch_type:
                    df[col] = df[col].astype("float64")
                else:
                    df[col] = df[col].astype("string")

            # Reorder cột
            df = df[ch_cols]

            # Insert bằng insert_df (an toàn hơn insert_arrow)
            client.insert_df(table_name, df)
            context.log.info(f"Inserted {len(df)} records into {table_name}")

    def load_input(self, context: InputContext) -> pd.DataFrame:
        table_name = self._get_table_name(context)
        with connect_to_clickhouse(self.config) as client:
            query = f"SELECT * FROM {table_name}"
            return client.query_df(query)
