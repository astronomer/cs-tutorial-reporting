from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator, PostgresHook
from airflow.utils.task_group import TaskGroup
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
from datetime import datetime, timedelta
import json
from operators.airflow_to_gcs import AirflowToGCSOperator
from operators.gcs_to_postgres import GCSToPostgres

headers = {
    "Content-Type": "application/json",
    "Accept": "*/*",
}
AIRFLOW_USERNAME = "admin"
AIRFLOW_PASSWORD = "admin"
AIRFLOW_WEBSERVER_HOST = "webserver"
AIRFLOW_WEBSERVER_PORT = "8080"
GCP_CONN_ID = "google_cloud_storage"
BUCKET = "customer-success-reporting"
PG_CONN_ID = "my_postgres_conn_id"
pg_hook = PostgresHook(postgres_conn_id=PG_CONN_ID)


def get_existing_dag_info(ti, **kwargs):
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT dag_id FROM rpt.dag")
    dags = cursor.fetchall()
    ti.xcom_push(key="DAG_IDS", value=json.dumps(dags))


def set_max_dag_run_start(ti, **kwargs):
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT Max(start_date) FROM rpt.dag_run;")
    max_dag_start = cursor.fetchone()
    ti.xcom_push(key="MAX_DAG_START", value=str(max_dag_start[0]))


def set_max_task_instance(ti, **kwargs):
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT Max(start_date) FROM rpt.task_instance;")
    max_task_start = cursor.fetchone()
    ti.xcom_push(key="MAX_TASK_START", value=str(max_task_start[0]))


with DAG(
    "reporting_dag",
    start_date=datetime(2019, 1, 1),
    max_active_runs=3,
    schedule_interval=None,
    catchup=False,
) as dag:
    t0 = PostgresOperator(task_id="ddl", sql="sql/rpt.sql", postgres_conn_id=PG_CONN_ID)
    with TaskGroup(group_id="dags") as tg1:
        t1 = PythonOperator(
            task_id="get_existing_dags", python_callable=get_existing_dag_info
        )
        t2 = AirflowToGCSOperator(
            task_id="dags_to_gcs",
            bucket=BUCKET,
            airflow_object="dags",
            gcp_conn_id=GCP_CONN_ID,
            dst="airflow/dags/{{ ts_nodash }}/",
        )
        t3 = GCSToPostgres(
            task_id="dag_gcs_to_postgres",
            bucket=BUCKET,
            destination_table="rpt.dag",
            source_format="JSON",
            source_objects="airflow/dags/{{ ts_nodash }}/dags.json",
            google_cloud_storage_conn_id=GCP_CONN_ID,
            pg_conn_id=PG_CONN_ID,
            pk_col="dag_id",
            schema_fields=[
                "dag_id",
                "is_paused",
                "is_subdag",
                "is_active",
                "fileloc",
                "file_token",
                "owners",
                "description",
                "root_dag_id",
                "schedule_interval",
            ],
        )
        t1 >> t2 >> t3
    with TaskGroup(group_id="dag_runs") as tg2:
        t4 = PythonOperator(
            task_id="set_max_dag_run_start", python_callable=set_max_dag_run_start
        )
        t5 = AirflowToGCSOperator(
            task_id="dag_runs_to_gcs",
            bucket=BUCKET,
            batch_size=10000,
            airflow_object="dagRuns",
            gcp_conn_id=GCP_CONN_ID,
            last_upload_date="{{ti.xcom_pull(task_ids='dag_runs.set_max_dag_run_start', key='MAX_DAG_START')}}",
            dst="airflow/dag_runs/{{ ts_nodash }}/",
        )
        t6 = GCSListObjectsOperator(
            task_id="list_dag_run_objects",
            bucket=BUCKET,
            gcp_conn_id=GCP_CONN_ID,
            prefix="airflow/dag_runs/{{ ts_nodash }}/",
        )
        t7 = GCSToPostgres(
            task_id="dag_run_gcs_to_postgres",
            bucket=BUCKET,
            destination_table="rpt.dag_run",
            source_format="JSON",
            source_objects=t6.output,
            google_cloud_storage_conn_id=GCP_CONN_ID,
            pg_conn_id=PG_CONN_ID,
            schema_fields=[
                "dag_id",
                "dag_run_id",
                "end_date",
                "execution_date",
                "external_trigger",
                "logical_date",
                "start_date",
                "state",
            ],
        )
        t4 >> t5 >> t6 >> t7
    with TaskGroup(group_id="task_instances") as tg3:

        t8 = PythonOperator(
            task_id="set_max_task_instance", python_callable=set_max_task_instance
        )
        t9 = AirflowToGCSOperator(
            task_id="task_instance_to_gcs",
            bucket=BUCKET,
            airflow_object="taskInstances",
            gcp_conn_id=GCP_CONN_ID,
            last_upload_date="{{ti.xcom_pull(task_ids='task_instances.set_max_task_instance', key='MAX_TASK_START')}}",
            dst="airflow/task_instance/{{ ts_nodash }}/",
        )
        t10 = GCSListObjectsOperator(
            task_id="list_task_instance_objects",
            bucket=BUCKET,
            gcp_conn_id=GCP_CONN_ID,
            prefix="airflow/task_instance/{{ ts_nodash }}/",
        )
        t11 = GCSToPostgres(
            task_id="task_instance_gcs_to_postgres",
            bucket=BUCKET,
            destination_table="rpt.task_instance",
            source_format="JSON",
            source_objects=t10.output,
            google_cloud_storage_conn_id=GCP_CONN_ID,
            pg_conn_id=PG_CONN_ID,
            schema_fields=[
                "dag_id",
                "task_id",
                "execution_date",
                "start_date",
                "end_date",
                "duration",
                "state",
                "try_number",
                "max_tries",
                "hostname",
                "unixname",
                "pool",
                "pool_slots",
                "queue",
                "priority_weight",
                "operator",
                "queued_when",
                "pid",
                "executor_config",
            ],
        )
        t8 >> t9 >> t10 >> t11

    t0 >> tg1
    t0 >> tg2
    t0 >> tg3
