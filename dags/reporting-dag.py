# from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator, PostgresHook
from airflow.utils.task_group import TaskGroup
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
from datetime import datetime, timedelta
import json
from operators.airflow_to_gcs import AirflowToGCSOperator
from operators.gcs_to_postgres import GCSToPostgres
from airflow.decorators import dag, task


@dag(
    start_date=datetime(2019, 1, 1),
    max_active_runs=3,
    schedule_interval=None,
    catchup=False,
    template_searchpath=f"include/sql/",
)
def reporting_dag():
    pg_hook = PostgresHook(postgres_conn_id="my_postgres_conn_id")

    @task
    def get_existing_dag_info():
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute("SELECT dag_id FROM rpt.dag")
        dags = cursor.fetchall()
        return json.dumps(dags)

    @task
    def set_max_dag_run_start():
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute("SELECT Max(start_date) FROM rpt.dag_run;")
        max_dag_start = cursor.fetchone()
        return str(max_dag_start[0])

    @task
    def set_max_task_instance():
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute("SELECT Max(start_date) FROM rpt.task_instance;")
        max_task_start = cursor.fetchone()
        return max_task_start[0]

    ddl = PostgresOperator(
        task_id="ddl", sql="rpt.sql", postgres_conn_id="my_postgres_conn_id"
    )
    with TaskGroup(group_id="dags") as dags:
        get_existing_dags = get_existing_dag_info()
        dags_to_gcs = AirflowToGCSOperator(
            task_id="dags_to_gcs",
            bucket="customer-success-reporting",
            airflow_object="dags",
            gcp_conn_id="google_cloud_storage",
            dst="airflow/dags/{{ ts_nodash }}/",
        )
        dag_gcs_to_postgres = GCSToPostgres(
            task_id="dag_gcs_to_postgres",
            bucket="customer-success-reporting",
            destination_table="rpt.dag",
            source_format="JSON",
            source_objects="airflow/dags/{{ ts_nodash }}/dags.json",
            google_cloud_storage_conn_id="google_cloud_storage",
            pg_conn_id="my_postgres_conn_id",
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
        get_existing_dags >> dags_to_gcs >> dag_gcs_to_postgres
    with TaskGroup(group_id="dag_runs") as dag_runs:
        set_max_dag_run_start = set_max_dag_run_start()
        dag_runs_to_gcs = AirflowToGCSOperator(
            task_id="dag_runs_to_gcs",
            bucket="customer-success-reporting",
            batch_size=10000,
            airflow_object="dagRuns",
            gcp_conn_id="google_cloud_storage",
            last_upload_date=set_max_dag_run_start,
            dst="airflow/dag_runs/{{ ts_nodash }}/",
        )
        list_dag_run_objects = GCSListObjectsOperator(
            task_id="list_dag_run_objects",
            bucket="customer-success-reporting",
            gcp_conn_id="google_cloud_storage",
            prefix="airflow/dag_runs/{{ ts_nodash }}/",
        )
        dag_run_gcs_to_postgres = GCSToPostgres(
            task_id="dag_run_gcs_to_postgres",
            bucket="customer-success-reporting",
            destination_table="rpt.dag_run",
            source_format="JSON",
            source_objects=list_dag_run_objects.output,
            google_cloud_storage_conn_id="google_cloud_storage",
            pg_conn_id="my_postgres_conn_id",
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
        (
            set_max_dag_run_start
            >> dag_runs_to_gcs
            >> list_dag_run_objects
            >> dag_run_gcs_to_postgres
        )
    with TaskGroup(group_id="task_instances") as task_instances:

        set_max_task_instance = set_max_task_instance()
        task_instance_to_gcs = AirflowToGCSOperator(
            task_id="task_instance_to_gcs",
            bucket="customer-success-reporting",
            airflow_object="taskInstances",
            gcp_conn_id="google_cloud_storage",
            last_upload_date=set_max_task_instance,
            dst="airflow/task_instance/{{ ts_nodash }}/",
        )
        list_task_instance_objects = GCSListObjectsOperator(
            task_id="list_task_instance_objects",
            bucket="customer-success-reporting",
            gcp_conn_id="google_cloud_storage",
            prefix="airflow/task_instance/{{ ts_nodash }}/",
        )
        task_instance_gcs_to_postgres = GCSToPostgres(
            task_id="task_instance_gcs_to_postgres",
            bucket="customer-success-reporting",
            destination_table="rpt.task_instance",
            source_format="JSON",
            source_objects=list_task_instance_objects.output,
            google_cloud_storage_conn_id="google_cloud_storage",
            pg_conn_id="my_postgres_conn_id",
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
        (
            set_max_task_instance
            >> task_instance_to_gcs
            >> list_task_instance_objects
            >> task_instance_gcs_to_postgres
        )

    ddl >> dags
    ddl >> dag_runs
    ddl >> task_instances


reporting_dag = reporting_dag()
