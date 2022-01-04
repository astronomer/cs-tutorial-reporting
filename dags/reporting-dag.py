from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator, PostgresHook
from airflow.version import version
from datetime import datetime, timedelta
import requests
import csv
import json
import os

headers = {"Content-Type": "application/json",
           "Accept": "*/*",
           }
AIRFLOW_USERNAME = 'admin'
AIRFLOW_PASSWORD = 'admin'
AIRFLOW_WEBSERVER_HOST = 'webserver'
AIRFLOW_WEBSERVER_PORT = '8080'
BATCH_SIZE=1000
pg_hook = PostgresHook(postgres_conn_id='my_postgres_conn_id')


def get_existing_dag_info(ti,**kwargs):
    conn= pg_hook.get_conn()
    cursor=conn.cursor()
    cursor.execute("SELECT dag_id FROM rpt.dag")
    dags = cursor.fetchall()
    ti.xcom_push(key='DAG_IDS',value=json.dumps(dags))
    # pg_hook.copy_expert(query, '/usr/local/airflow/dags.tsv')

def set_dag_info(ti,**kwargs):
    with open('dags.tsv', 'w', ) as tsvfile:
        var_return = requests.get(f"http://{AIRFLOW_WEBSERVER_HOST}:{AIRFLOW_WEBSERVER_PORT}/api/v1/dags",
                                  headers=headers,
                                  auth=(AIRFLOW_USERNAME, AIRFLOW_PASSWORD))
        fields = ['dag_id',
                  'is_paused',
                  'is_subdag',
                  'is_active',
                  'fileloc',
                  'file_token',
                  'owners',
                  'description',
                  'root_dag_id',
                  'schedule_interval']
        writer = csv.DictWriter(tsvfile, fieldnames=fields)
        writer.writeheader()
        existing_dags=ti.xcom_pull(key='DAG_IDS',task_ids=['get_existing_dags'])
        decoder=json.JSONDecoder()
        existing_dags=decoder.decode(str(existing_dags[0]))
        for dag in var_return.json()['dags']:
            dag_dict = requests.get(
                f"http://{AIRFLOW_WEBSERVER_HOST}:{AIRFLOW_WEBSERVER_PORT}/api/v1/dags/{dag['dag_id']}",
                headers=headers,
                auth=(AIRFLOW_USERNAME, AIRFLOW_PASSWORD)).json()
            row = {'dag_id': dag_dict['dag_id'],
                   'is_paused': str(dag_dict['is_paused']),
                   'is_subdag': dag_dict['is_subdag'],
                   'is_active': dag_dict['is_active'],
                   'fileloc': dag_dict['fileloc'],
                   'file_token': dag_dict['file_token'],
                   'owners': dag_dict['owners'],
                   'description': dag_dict['description'],
                   'root_dag_id': dag_dict['root_dag_id'],
                   'schedule_interval': dag_dict['schedule_interval']}
            if [row['dag_id']] not in existing_dags:
                writer.writerow(row)
                print(row)
    query = "COPY rpt.dag FROM STDIN WITH CSV HEADER NULL AS '' "
    pg_hook.copy_expert(query, '/usr/local/airflow/dags.tsv')
    os.remove('/usr/local/airflow/dags.tsv')

def get_latest_dag_run_info(ti,**kwargs):
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT Max(start_date) FROM rpt.dag_run;")
    max_dag_start = cursor.fetchone()
    ti.xcom_push(key='MAX_DAG_START',value=str(max_dag_start[0]))

def set_dag_run_info(ti,**kwargs):
    with open('dag_runs.tsv', 'w', ) as tsvfile:
        max_dag_start=ti.xcom_pull(key='MAX_DAG_START',task_ids=['get_max_dag_run'])
        print(max_dag_start[0])
        print(max_dag_start)
        dt = datetime.fromisoformat(max_dag_start[0])
        delta = timedelta(microseconds=1)
        final_dt = dt + delta
        data = {'page_limit': BATCH_SIZE,
                'page_offset': 0,
                'start_date_gte':final_dt.isoformat()}
        var_return = requests.post(
            f"http://{AIRFLOW_WEBSERVER_HOST}:{AIRFLOW_WEBSERVER_PORT}/api/v1/dags/~/dagRuns/list",
            headers=headers, data=json.dumps(data),
            auth=(AIRFLOW_USERNAME, AIRFLOW_PASSWORD))

        fields = ["dag_id",
                  "dag_run_id",
                  "end_date",
                  "execution_date",
                  "external_trigger",
                  "logical_date",
                  "start_date",
                  "state"]

        writer = csv.DictWriter(tsvfile, fieldnames=fields)
        writer.writeheader()
        for dag_run in var_return.json()['dag_runs']:
            row = {
                "dag_id": dag_run['dag_id'],
                "dag_run_id": dag_run['dag_run_id'],
                "end_date": dag_run['end_date'],
                "execution_date": dag_run['execution_date'],
                "external_trigger": dag_run['external_trigger'],
                "logical_date": dag_run['logical_date'],
                "start_date": dag_run['start_date'],
                "state": dag_run['state'],
            }
            writer.writerow(row)
            # print(row)
        total_entries = var_return.json()['total_entries']
        offset = BATCH_SIZE
        while offset < total_entries:
            data = {'page_limit': BATCH_SIZE,
                    'page_offset': offset}

            var_return = requests.post(
                f"http://{AIRFLOW_WEBSERVER_HOST}:{AIRFLOW_WEBSERVER_PORT}/api/v1/dags/~/dagRuns/list",
                headers=headers, data=json.dumps(data),
                auth=(AIRFLOW_USERNAME, AIRFLOW_PASSWORD))
            for dag_run in var_return.json()['dag_runs']:
                row = {
                    "dag_id": dag_run['dag_id'],
                    "dag_run_id": dag_run['dag_run_id'],
                    "end_date": dag_run['end_date'],
                    "execution_date": dag_run['execution_date'],
                    "external_trigger": dag_run['external_trigger'],
                    "logical_date": dag_run['logical_date'],
                    "start_date": dag_run['start_date'],
                    "state": dag_run['state'],
                }
                writer.writerow(row)
                print(row)
            offset += BATCH_SIZE
    query = "COPY rpt.dag_run FROM STDIN WITH CSV HEADER NULL AS '' "
    pg_hook.copy_expert(query, '/usr/local/airflow/dag_runs.tsv')
    os.remove('/usr/local/airflow/dag_runs.tsv')

def get_max_task_instance(ti,**kwargs):
    conn= pg_hook.get_conn()
    cursor=conn.cursor()
    cursor.execute("SELECT Max(start_date) FROM rpt.task_instance;")
    max_task_start = cursor.fetchone()
    ti.xcom_push(key='MAX_TASK_START',value=str(max_task_start[0]))

def set_task_instance_info(ti,**kwargs):
    with open('task_instance.tsv', 'w', ) as tsvfile:
        max_task_start = ti.xcom_pull(key='MAX_TASK_START', task_ids=['get_max_dag_run'])
        print(max_task_start[0])
        print(max_task_start)
        dt = datetime.fromisoformat(max_task_start[0])
        delta = timedelta(microseconds=1)
        final_dt = dt + delta
        #
        # data={}
        data = {
            # 'page_limit': BATCH_SIZE,
            #     'page_offset': 0,
                'start_date_gte': final_dt.isoformat()}
        var_return = requests.post(
            f"http://{AIRFLOW_WEBSERVER_HOST}:{AIRFLOW_WEBSERVER_PORT}/api/v1/dags/~/dagRuns/~/taskInstances/list",
            headers=headers, data=json.dumps(data),
            auth=(AIRFLOW_USERNAME, AIRFLOW_PASSWORD))
        fields = ['dag_id',
                  'task_id',
                  'execution_date',
                  'start_date',
                  'end_date',
                  'duration',
                  'state',
                  'try_number',
                  'max_tries',
                  'hostname',
                  'unixname',
                  'pool',
                  'pool_slots',
                  'queue',
                  'priority_weight',
                  'operator',
                  'queued_when',
                  'pid',
                  'executor_config']
        writer = csv.DictWriter(tsvfile, fieldnames=fields)
        writer.writeheader()
        for task_instance in var_return.json()['task_instances']:
            row = {'dag_id': task_instance['dag_id'],
                   'task_id': task_instance['task_id'],
                   'execution_date': task_instance['execution_date'],
                   'start_date': task_instance['start_date'],
                   'end_date': task_instance['end_date'],
                   'duration': task_instance['duration'],
                   'state': task_instance['state'],
                   'try_number': task_instance['try_number'],
                   'max_tries': task_instance['max_tries'],
                   'hostname': task_instance['hostname'],
                   'unixname': task_instance['unixname'],
                   'pool': task_instance['pool'],
                   'pool_slots': task_instance['pool_slots'],
                   'queue': task_instance['queue'],
                   'priority_weight': task_instance['priority_weight'],
                   'operator': task_instance['operator'],
                   'queued_when': task_instance['queued_when'],
                   'pid': task_instance['pid'],
                   'executor_config': task_instance['executor_config']
                   }
            writer.writerow(row)

        query = "COPY rpt.task_instance FROM STDIN WITH CSV HEADER NULL AS '' "
        pg_hook.copy_expert(query, '/usr/local/airflow/task_instance.tsv')
        # os.remove('/usr/local/airflow/task_instance.tsv')



# Default settings applied to all tasks
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Using a DAG context manager, you don't have to specify the dag property of each task
with DAG('reporting_dag',
         start_date=datetime(2019, 1, 1),
         max_active_runs=3,
         schedule_interval=None,  # https://airflow.apache.org/docs/stable/scheduler.html#dag-runs
         default_args=default_args,
         # catchup=False # enable if you don't want historical dag runs to run
         ) as dag:
    t0 = PostgresOperator(
        task_id='ddl',
        sql='sql/rpt.sql',
        postgres_conn_id='my_postgres_conn_id'
    )
    t1=PythonOperator(
    task_id="get_existing_dags",
    python_callable= get_existing_dag_info
    # parameters={"begin_date": "2020-01-01", "end_date": "2020-12-31"},
    )

    t2 = PythonOperator(
        task_id='set_dag_info',
        python_callable=set_dag_info
    )
    t3 = PythonOperator(
        task_id='get_max_dag_run',
        python_callable=get_latest_dag_run_info
    )
    t4 = PythonOperator(
        task_id='set_dag_run_info',
        python_callable=set_dag_run_info
    )
    t5 = PythonOperator(
        task_id='get_max_task_instance',
        python_callable=get_max_task_instance
    )
    t6 = PythonOperator(
        task_id='set_task_instance_info',
        python_callable=set_task_instance_info
    )

    t0 >> t1 >> t2 >> t3 >> t4 >>t5 >> t6
