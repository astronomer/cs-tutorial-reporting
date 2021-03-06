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
"""This module contains operator for loading Airflow dag,dagrun, and task instance information to GCS."""
import os
import warnings
from glob import glob
from typing import TYPE_CHECKING, Optional, Sequence, Union
import requests
from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from datetime import datetime, timedelta
import json
import csv

if TYPE_CHECKING:
    from airflow.utils.context import Context


class AirflowToGCSOperator(BaseOperator):
    """
    Upload information about airflow dags, dagruns, and task instances to a gcs bucket

    :param dst: Destination path within the specified bucket on GCS (e.g. /path/to/file.ext).
        If multiple files are being uploaded, specify object prefix with trailing backslash
        (e.g. /path/to/directory/) (templated)
    :param bucket: The bucket to upload to. (templated)
    :param airflow_user: The airflow you will connect with
    :param airflow_pass: password for the user
    :param airflow_host: the hostname or ip of you airflow deployment
    :param airflow_port: the port of you airflow deployment
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
    :param google_cloud_storage_conn_id: (Deprecated) The connection ID used to connect to Google Cloud.
        This parameter has been deprecated. You should pass the gcp_conn_id parameter instead.
    :param mime_type: The mime-type string
    :param delegate_to: The account to impersonate, if any
    :param gzip: Allows for file to be compressed and uploaded as gzip
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param airflow_object: either dags, dagRuns, or taskInstances. The airlfow object being reported on
    :param last_upload_date: load everything after this date
    :param batch_size: number of entries in each individual file
    """

    template_fields: Sequence[str] = (
        "dst",
        "bucket",
        "impersonation_chain",
        "last_upload_date",
    )

    def __init__(
        self,
        *,
        dst,
        bucket,
        airflow_user="admin",
        airflow_pass="admin",
        airflow_host="webserver",
        airflow_port=8080,
        gcp_conn_id="google_cloud_default",
        google_cloud_storage_conn_id=None,
        mime_type="application/octet-stream",
        delegate_to=None,
        gzip=False,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        airflow_object="dags",
        last_upload_date=None,
        batch_size=1000,
        **kwargs,
    ):
        super().__init__(**kwargs)

        if google_cloud_storage_conn_id:
            warnings.warn(
                "The google_cloud_storage_conn_id parameter has been deprecated. You should pass "
                "the gcp_conn_id parameter.",
                DeprecationWarning,
                stacklevel=3,
            )
            gcp_conn_id = google_cloud_storage_conn_id

        self.dst = dst
        self.bucket = bucket
        self.airflow_user = airflow_user
        self.airflow_pass = airflow_pass
        self.airflow_host = airflow_host
        self.airflow_port = airflow_port
        self.gcp_conn_id = gcp_conn_id
        self.airflow_object = airflow_object
        self.last_upload_date = last_upload_date
        self.mime_type = mime_type
        self.delegate_to = delegate_to
        self.gzip = gzip
        self.batch_size = batch_size
        self.impersonation_chain = impersonation_chain

    def execute(self, context: "Context"):
        """Uploads a file or list of files to Google Cloud Storage"""
        hook = GCSHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )
        headers = {
            "Content-Type": "application/json",
            "Accept": "*/*",
        }
        if self.airflow_object == "dags":
            var_return = requests.get(
                f"http://{self.airflow_host}:{self.airflow_port}/api/v1/dags",
                headers=headers,
                auth=(self.airflow_user, self.airflow_pass),
            )
            for dag in var_return.json()["dags"]:
                dag_dict = requests.get(
                    f"http://{self.airflow_host}:{self.airflow_port}/api/v1/dags/{dag['dag_id']}",
                    headers=headers,
                    auth=(self.airflow_user, self.airflow_pass),
                ).json()
                row = {
                    "dag_id": dag_dict["dag_id"],
                    "is_paused": str(dag_dict["is_paused"]),
                    "is_subdag": dag_dict["is_subdag"],
                    "is_active": dag_dict["is_active"],
                    "fileloc": dag_dict["fileloc"],
                    "file_token": dag_dict["file_token"],
                    "owners": dag_dict["owners"],
                    "description": dag_dict["description"],
                    "root_dag_id": dag_dict["root_dag_id"],
                    "schedule_interval": dag_dict["schedule_interval"],
                }
            hook.upload(
                bucket_name=self.bucket,
                data=json.dumps(info),
                object_name=self.dst + "dags.json",
                mime_type=self.mime_type,
                gzip=self.gzip,
            )

        elif self.airflow_object == "taskInstances":
            if self.last_upload_date == "None":
                data = {"page_limit": self.batch_size, "page_offset": 0}
            else:
                dt = datetime.fromisoformat(self.last_upload_date)
                delta = timedelta(microseconds=1)
                final_dt = dt + delta
                data = {
                    "page_limit": self.batch_size,
                    "page_offset": 0,
                    "start_date_gte": final_dt.isoformat(),
                }
                self.log.info(data)
            var_return = requests.post(
                f"http://{self.airflow_host}:{self.airflow_port}/api/v1/dags/~/dagRuns/~/taskInstances/list",
                headers=headers,
                data=json.dumps(data),
                auth=(self.airflow_user, self.airflow_pass),
            )

            self.log.info(var_return.json())
            for task_instance in var_return.json()["task_instances"]:
                row = {
                    "dag_id": task_instance["dag_id"],
                    "task_id": task_instance["task_id"],
                    "execution_date": task_instance["execution_date"],
                    "start_date": task_instance["start_date"],
                    "end_date": task_instance["end_date"],
                    "duration": task_instance["duration"],
                    "state": task_instance["state"],
                    "try_number": task_instance["try_number"],
                    "max_tries": task_instance["max_tries"],
                    "hostname": task_instance["hostname"],
                    "unixname": task_instance["unixname"],
                    "pool": task_instance["pool"],
                    "pool_slots": task_instance["pool_slots"],
                    "queue": task_instance["queue"],
                    "priority_weight": task_instance["priority_weight"],
                    "operator": task_instance["operator"],
                    "queued_when": task_instance["queued_when"],
                    "pid": task_instance["pid"],
                    "executor_config": task_instance["executor_config"],
                }
                info.append(row)
            hook.upload(
                bucket_name=self.bucket,
                data=json.dumps(info),
                object_name=self.dst + "taskInstances1.json",
                mime_type=self.mime_type,
                gzip=self.gzip,
            )

        elif self.airflow_object == "dagRuns":
            if self.last_upload_date == "None":
                data = {"page_limit": self.batch_size, "page_offset": 0}
            else:
                dt = datetime.fromisoformat(self.last_upload_date)
                delta = timedelta(microseconds=1)
                final_dt = dt + delta
                data = {
                    "page_limit": self.batch_size,
                    "page_offset": 0,
                    "start_date_gte": final_dt.isoformat(),
                }
            var_return = requests.post(
                f"http://{self.airflow_host}:{self.airflow_port}/api/v1/dags/~/dagRuns/list",
                headers=headers,
                data=json.dumps(data),
                auth=(self.airflow_user, self.airflow_pass),
            )
            fields = [
                "dag_id",
                "dag_run_id",
                "end_date",
                "execution_date",
                "external_trigger",
                "logical_date",
                "start_date",
                "state",
            ]
            for dag_run in var_return.json()["dag_runs"]:
                row = {
                    "dag_id": dag_run["dag_id"],
                    "dag_run_id": dag_run["dag_run_id"],
                    "end_date": dag_run["end_date"],
                    "execution_date": dag_run["execution_date"],
                    "external_trigger": dag_run["external_trigger"],
                    "logical_date": dag_run["logical_date"],
                    "start_date": dag_run["start_date"],
                    "state": dag_run["state"],
                }
                info.append(row)
            hook.upload(
                bucket_name=self.bucket,
                data=json.dumps(info),
                object_name=self.dst + "dagRuns1.json",
                mime_type=self.mime_type,
                gzip=self.gzip,
            )
            total_entries = var_return.json()["total_entries"]
            offset = self.batch_size
            cnt = 1
            while offset < total_entries:
                cnt += 1
                info = []
                data = {"page_limit": self.batch_size, "page_offset": offset}

                var_return = requests.post(
                    f"http://{self.airflow_host}:{self.airflow_port}/api/v1/dags/~/dagRuns/list",
                    headers=headers,
                    data=json.dumps(data),
                    auth=(self.airflow_user, self.airflow_pass),
                )
                for dag_run in var_return.json()["dag_runs"]:
                    row = {
                        "dag_id": dag_run["dag_id"],
                        "dag_run_id": dag_run["dag_run_id"],
                        "end_date": dag_run["end_date"],
                        "execution_date": dag_run["execution_date"],
                        "external_trigger": dag_run["external_trigger"],
                        "logical_date": dag_run["logical_date"],
                        "start_date": dag_run["start_date"],
                        "state": dag_run["state"],
                    }
                    info.append(row)
                hook.upload(
                    bucket_name=self.bucket,
                    data=json.dumps(info),
                    object_name=self.dst + f"dagRuns{cnt}.json",
                    mime_type=self.mime_type,
                    gzip=self.gzip,
                )
                offset += self.batch_size
