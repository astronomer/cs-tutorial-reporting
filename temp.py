import requests
import csv
import json
import psycopg2
import math
from datetime import datetime, timedelta

headers = {"Content-Type": "application/json",
           "Accept": "*/*",
           }
AIRFLOW_USERNAME = 'admin'
AIRFLOW_PASSWORD = 'admin'
AIRFLOW_WEBSERVER_HOST = 'localhost'
AIRFLOW_WEBSERVER_PORT = '8080'
BATCH_SIZE=1000
# conn=psycopg2.connect(
#     dbname ='postgres',
#     user ='postgres',
#     password ='postgres',
#     host ='localhost',
#     port=5432,
# )
# cur = conn.cursor()
# cur.execute("SELECT dag_id FROM rpt.dag")
# dags=cur.fetchall()
# dags=json.dumps(dags)
# decoder=json.JSONDecoder()
# existing_dags=decoder.decode(dags)
# var_return = requests.get(f"http://{AIRFLOW_WEBSERVER_HOST}:{AIRFLOW_WEBSERVER_PORT}/api/v1/dags",
#                           headers=headers,
#                           auth=(AIRFLOW_USERNAME, AIRFLOW_PASSWORD))
body={
  # "order_by": "dag_run_id",
  # "page_offset": 0,
  # "page_limit": 100,
  # "dag_ids": [
  #   "example_dag"
  # ],
  # "execution_date_gte": "2018-08-24T14:15:22Z",
  # "execution_date_lte": "2019-08-24T14:15:22Z",
  # "start_date_gte": "2018-08-24T14:15:22Z",
  # "start_date_lte": "2019-08-24T14:15:22Z",
  # "end_date_gte": "2018-08-24T14:15:22Z"
  # "end_date_lte": "2019-08-24T14:15:22Z"
}

# var_return=requests.post(  f"http://{AIRFLOW_WEBSERVER_HOST}:{AIRFLOW_WEBSERVER_PORT}/api/v1/dags/~/dagRuns/~/taskInstances/list",
#                             headers=headers,data=json.dumps(body),
#             auth=(AIRFLOW_USERNAME, AIRFLOW_PASSWORD))
# print(var_return.json())

initial_time = '2021-12-06T18:16:21.215528+00:00'
dt=datetime.fromisoformat(initial_time)
delta=timedelta(microseconds=1)
final_dt=dt+delta
final_dt.isoformat()# dt.microsecond+=1
final_time ='2021-12-06T18:16:21.215529+00:00'
with open('dag_runs.tsv', 'w', ) as tsvfile:
    # data={'start_date_gte':'2021-12-06 13:54:27.009089+00:00'}
    data = {'page_limit':BATCH_SIZE,
            'page_offset':0}
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
        print(row)
    total_entries=var_return.json()['total_entries']
    offset=BATCH_SIZE
    while offset<total_entries:
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
        offset+=BATCH_SIZE
query = "COPY rpt.dag_run FROM STDIN WITH CSV HEADER NULL AS '' "

