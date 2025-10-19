import os
from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import Union

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from dagster import IOManager, OutputContext, InputContext
from minio import Minio


@contextmanager
def connect_minio(config):
    client = Minio(
        endpoint=config.get("endpoint_url"),
        access_key=config.get("aws_access_key_id"),
        secret_key=config.get("aws_secret_access_key"),
        secure=False,
    )
    try:
        yield client
    except Exception:
        raise  


class MinIOIOManager(IOManager):
    def __init__(self, config):
        self._config = config

    def _get_path(self, context: Union[InputContext, OutputContext]):
        layer, schema, folder, table = context.asset_key.path
        base_key = "/".join([layer, schema, folder, table.replace(f"{layer}_{folder}_", "")])

        # if has the partition
        if context.has_asset_partitions:
            partition_key = context.asset_partition_key
            start_day = datetime.strptime (partition_key, "%Y-%m-%d")

            end_day = (start_day + timedelta (weeks=1)).strftime ("%Y-%m-%d")
            key = f"{base_key}/partition={partition_key}_to_{end_day}/data.parquet"
        else:
            key = f"{base_key}.parquet"

        tmp_file_path = f"/tmp/{datetime.now().strftime('%Y%m%d%H%M%S')}_{table}.parquet"
        return key, tmp_file_path

    def handle_output(self, context: OutputContext, obj: pd.DataFrame):
        key_name, tmp_file_path = self._get_path(context)

        # convert from DataFrame -> Parquet
        table = pa.Table.from_pandas(obj)
        pq.write_table(table, tmp_file_path)

        bucket_name = self._config.get("bucket")

        try:
            with connect_minio(self._config) as client:
                # create bucket if not exists
                if not client.bucket_exists(bucket_name):
                    client.make_bucket(bucket_name)
                    context.log.info(f"Created bucket {bucket_name}")
                else:
                    context.log.debug(f"Bucket {bucket_name} already exists")

                # Upload to MinIO
                client.fput_object(bucket_name, key_name, tmp_file_path)
                context.log.info(f"Uploaded {key_name} to bucket {bucket_name}")

                # metadata
                context.add_output_metadata({
                    "bucket": bucket_name,
                    "key": key_name,
                    "records": len(obj),
                    "tmp_path": tmp_file_path,
                })

        finally:
            if os.path.exists(tmp_file_path):
                os.remove(tmp_file_path)

    def load_input(self, context: InputContext) -> pd.DataFrame:
        key_name, tmp_file_path = self._get_path(context)
        bucket_name = self._config.get("bucket")

        try:
            with connect_minio(self._config) as client:
                if not client.bucket_exists(bucket_name):
                    raise FileNotFoundError(f"Bucket {bucket_name} does not exist.")

                client.fget_object(bucket_name, key_name, tmp_file_path)

                df = pd.read_parquet(tmp_file_path)
                return df

        finally:
            if os.path.exists(tmp_file_path):
                os.remove(tmp_file_path)