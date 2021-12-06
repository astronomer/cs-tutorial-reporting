from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator, PostgresHook
from airflow.version import version
from datetime import datetime, timedelta
import requests
import csv

headers = {"Content-Type": "application/json",
           "Accept": "*/*",
           }
AIRFLOW_USERNAME = 'admin'
AIRFLOW_PASSWORD = 'admin'
AIRFLOW_WEBSERVER_HOST = 'webserver'
AIRFLOW_WEBSERVER_PORT = '8080'
def get_dag_info(**kwargs):
    with open('dags.tsv', 'w', ) as tsvfile:
        var_return=requests.get(  f"http://{AIRFLOW_WEBSERVER_HOST}:{AIRFLOW_WEBSERVER_PORT}/api/v1/dags",
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
        writer = csv.DictWriter(tsvfile, fieldnames=fields, delimiter='\t')
        # writer.writeheader()
        for dag in var_return.json()['dags']:
            dag_dict = requests.get(
                f"http://{AIRFLOW_WEBSERVER_HOST}:{AIRFLOW_WEBSERVER_PORT}/api/v1/dags/{dag['dag_id']}",
                headers=headers,
                auth=(AIRFLOW_USERNAME, AIRFLOW_PASSWORD)).json()
            row={'dag_id': dag_dict['dag_id'],
                             'is_paused': str(dag_dict['is_paused']),
                             'is_subdag': dag_dict['is_subdag'],
                             'is_active': dag_dict['is_active'],
                             'fileloc': dag_dict['fileloc'],
                             'file_token': dag_dict['file_token'],
                             'owners': dag_dict['owners'],
                             'description': dag_dict['description'],
                             'root_dag_id': dag_dict['root_dag_id'],
                             'schedule_interval': dag_dict['schedule_interval']}
            writer.writerow(row)
            print(row)
    pg_hook=PostgresHook(postgres_conn_id='my_postgres_conn_id')
    pg_hook.bulk_load(table='rpt.dag',tmp_file='dags.tsv')

    print(var_return.json())
    print('Here is the full DAG Run context. It is available because provide_context=True')
    print(kwargs)
def get_dag_run_info(**kwargs):

    with open('dag_runs.tsv', 'w', ) as tsvfile:
        var_return=requests.get(  f"http://{AIRFLOW_WEBSERVER_HOST}:{AIRFLOW_WEBSERVER_PORT}/api/v1/dags/~/dagRuns/list",
                            headers=headers,
            auth=(AIRFLOW_USERNAME, AIRFLOW_PASSWORD))
        fields = ["dag_run_id",
    "dag_id",
    "logical_date",
    "execution_date",
    "start_date",
    "end_date",
    "state",
    "external_trigger"]
        writer = csv.DictWriter(tsvfile, fieldnames=fields, delimiter='\t')
        # writer.writeheader()
        for dag_run in var_return.json()['dag_runs']:
            # dag_run_dict = requests.get(
            #     f"http://{AIRFLOW_WEBSERVER_HOST}:{AIRFLOW_WEBSERVER_PORT}/api/v1/dags/{dag['dag_id']}",
            #     headers=headers,
            #     auth=(AIRFLOW_USERNAME, AIRFLOW_PASSWORD)).json()
            row={
                "dag_run_id": dag_run['dag_id'],
                "dag_id": dag_run['dag_id'],
                "logical_date": dag_run['dag_id'],
                "execution_date": dag_run['dag_id'],
                "start_date": dag_run['dag_id'],
                "end_date": dag_run['dag_id'],
                "state": dag_run['dag_id'],
                "external_trigger": dag_run['dag_id']
              }
            writer.writerow(row)
            print(row)
    pg_hook=PostgresHook(postgres_conn_id='my_postgres_conn_id')
    pg_hook.bulk_load(table='rpt.dag_run',tmp_file='dag_runs.tsv')

def get_task_instance_info(**kwargs):

    var_return = requests.get(f"http://{AIRFLOW_WEBSERVER_HOST}:{AIRFLOW_WEBSERVER_PORT}/api/v1/dags",
                              headers=headers,
                              auth=(AIRFLOW_USERNAME, AIRFLOW_PASSWORD))
    print(var_return.json())
    print('Here is the full DAG Run context. It is available because provide_context=True')
    print(kwargs)

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
    t0= PostgresOperator(
        task_id='ddl',
        sql='sql/rpt.sql',
        postgres_conn_id='my_postgres_conn_id'
    )
    t1 = PythonOperator(
        task_id='get_dag_info',
        python_callable=get_dag_info
    )
    t2 = PythonOperator(
        task_id='get_dag_run_info',
        python_callable=get_dag_run_info
    )
    t3 = PythonOperator(
        task_id='get_task_instance_info',
        python_callable=get_task_instance_info
    )


    t0>>t1>>t2>>t3