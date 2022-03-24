#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""This module contains a Google Cloud Storage to Postgres operator."""

import json
import csv
import os
from typing import TYPE_CHECKING, Optional, Sequence, Union
from pathlib import Path
from airflow.models import BaseOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class GCSToPostgres(BaseOperator):
    """Add operator docstring"""

    template_fields: Sequence[str] = (
        "bucket",
        "source_objects",
        "schema_object",
        "impersonation_chain",
    )
    template_ext: Sequence[str] = (".sql",)
    ui_color = "#f0eee4"

    def __init__(
        self,
        *,
        bucket,
        source_objects,
        destination_table,
        pk_col=None,
        schema_fields=None,
        source_format="CSV",
        field_delimiter=",",
        max_bad_records=0,
        quote_character=None,
        allow_quoted_newlines=False,
        allow_jagged_rows=False,
        encoding="UTF-8",
        pg_conn_id="pg_default",
        google_cloud_storage_conn_id="google_cloud_default",
        delegate_to=None,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs,
    ):

        super().__init__(**kwargs)

        # GCS config
        self.bucket = bucket
        self.source_objects = source_objects

        self.destination_table = destination_table
        self.schema_fields = schema_fields
        self.source_format = source_format
        self.pk_col = pk_col
        self.field_delimiter = field_delimiter
        self.max_bad_records = max_bad_records
        self.quote_character = quote_character
        self.allow_quoted_newlines = allow_quoted_newlines
        self.allow_jagged_rows = allow_jagged_rows
        self.encoding = encoding

        self.pg_conn_id = pg_conn_id
        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id
        self.delegate_to = delegate_to

        self.impersonation_chain = impersonation_chain

    def execute(self, context: "Context"):
        pg_hook = PostgresHook(postgres_conn_id=self.pg_conn_id)
        gcs_hook = GCSHook(
            gcp_conn_id=self.google_cloud_storage_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )

        self.source_objects = (
            self.source_objects
            if isinstance(self.source_objects, list)
            else [self.source_objects]
        )
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        existing_pks = []
        if self.pk_col is not None:
            cursor.execute(f"SELECT {self.pk_col} FROM {self.destination_table};")
            pks = cursor.fetchall()
            self.log.info("these will not get loaded")
            self.log.info(pks)
            for pk in pks:
                self.log.info("pk" + str(pk))
                existing_pks.append(pk[0])
                self.log.info("existing_pks" + str(existing_pks))

        for source_object in self.source_objects:
            self.log.info("1")
            source_file = gcs_hook.download(self.bucket, source_object)
            self.log.info("2")
            self.log.info(source_file)
            source_file = json.loads(source_file.decode(self.encoding))
            self.log.info(source_file)
            self.log.info("3")
            self.log.info(source_file)
            self.log.info("4")
            self.log.info(f"/tmp/{source_object}")
            x = Path(f"/tmp/{source_object}").mkdir(parents=True, exist_ok=True)
            os.rmdir(f"/tmp/{source_object}")
            self.log.info(x)

            with open(f"/tmp/{source_object}", "w+") as dest_file:
                self.log.info("8")
                fields = self.schema_fields
                writer = csv.DictWriter(dest_file, fieldnames=fields)
                writer.writeheader()
                self.log.info("here some dags")
                self.log.info(source_file)
                for object in source_file:

                    row = {}
                    for field in self.schema_fields:
                        row[field] = object[field]
                    if self.pk_col is not None:
                        if row[self.pk_col] not in existing_pks:
                            writer.writerow(row)
                    else:
                        writer.writerow(row)
            query = (
                f"COPY {self.destination_table} FROM STDIN WITH CSV HEADER NULL AS '' "
            )
            pg_hook.copy_expert(query, f"/tmp/{source_object}")
            os.remove(f"/tmp/{source_object}")
