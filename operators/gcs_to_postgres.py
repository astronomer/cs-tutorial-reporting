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
    """Add operator docstring

    :param bucket: The bucket to load from. (templated)
    :param source_objects: String or List of Google Cloud Storage URIs to load from. (templated)
        If source_format is 'DATASTORE_BACKUP', the list must only contain a single URI.
    :param destination_table: the name of the postgres table you wish to load to
    :param pk the nme of the primary key
    :param schema_fields: If set, the schema field list as defined here:
        https://cloud.google.com/bigquery/docs/reference/v2/jobs#configuration.load
        Should not be set when source_format is 'DATASTORE_BACKUP'.
        Parameter must be defined if 'schema_object' is null and autodetect is False.
    :param source_format: File format to export.
    :param field_delimiter: the delimiter in you csv files
         :param max_bad_records: The maximum number of bad records that BigQuery can
        ignore when running the job.
    :param quote_character: The value that is used to quote data sections in a CSV file.
    :param ignore_unknown_values: [Optional] Indicates if BigQuery should allow
        extra values that are not represented in the table schema.
        If true, the extra values are ignored. If false, records with extra columns
        are treated as bad records, and if there are too many bad records, an
        invalid error is returned in the job result.
    :param allow_quoted_newlines: Whether to allow quoted newlines (true) or not (false).
    :param allow_jagged_rows: Accept rows that are missing trailing optional columns.
        The missing values are treated as nulls. If false, records with missing trailing
        columns are treated as bad records, and if there are too many bad records, an
        invalid error is returned in the job result. Only applicable to CSV, ignored
        for other formats.
    :param encoding: The character encoding of the data.
    :param pg_conn_id: the conn_id of the postgres db you to be loaded
    :param google_cloud_storage_conn_id: (Optional) The connection ID used to connect to Google Cloud
        and interact with the Google Cloud Storage service.
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

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
        pk=None,
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
        self.pk = pk
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
        if self.pk is not None:
            cursor.execute(f"SELECT {self.pk} FROM {self.destination_table};")
            pks = cursor.fetchall()
            self.log.info("these will not get loaded")
            self.log.info(pks)
        for source_object in self.source_objects:
            source_file = gcs_hook.download(self.bucket, source_object)
            source_file = json.loads(source_file.decode(self.encoding))
            temp_path = Path(f"/tmp/{source_object}").mkdir(parents=True, exist_ok=True)
            os.rmdir(f"/tmp/{source_object}")

            with open(f"/tmp/{source_object}", "w+") as dest_file:
                fields = self.schema_fields
                writer = csv.DictWriter(dest_file, fieldnames=fields)
                writer.writeheader()
                for object in source_file:
                    row = {}
                    for field in self.schema_fields:
                        row[field] = object[field]
                    if self.pk is not None:
                        if row[self.pk] not in existing_pks:
                            writer.writerow(row)
                    else:
                        writer.writerow(row)
            query = (
                f"COPY {self.destination_table} FROM STDIN WITH CSV HEADER NULL AS '' "
            )
            pg_hook.copy_expert(query, f"/tmp/{source_object}")
            os.remove(f"/tmp/{source_object}")
