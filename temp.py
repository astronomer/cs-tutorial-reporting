import requests
import csv

headers = {"Content-Type": "application/json",
           "Accept": "*/*",
           }
AIRFLOW_USERNAME = 'admin'
AIRFLOW_PASSWORD = 'admin'
AIRFLOW_WEBSERVER_HOST = 'localhost'
AIRFLOW_WEBSERVER_PORT = '8080'



var_return = requests.get(f"http://{AIRFLOW_WEBSERVER_HOST}:{AIRFLOW_WEBSERVER_PORT}/api/v1/dags",
                          headers=headers,
                          auth=(AIRFLOW_USERNAME, AIRFLOW_PASSWORD))
print(var_return.json())
with open('dags.tsv','w',)  as tsvfile:
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
    writer.writeheader()

    for dag in var_return.json()['dags']:

        dag_dict = requests.get(f"http://{AIRFLOW_WEBSERVER_HOST}:{AIRFLOW_WEBSERVER_PORT}/api/v1/dags/{dag['dag_id']}",
                                  headers=headers,
                                  auth=(AIRFLOW_USERNAME, AIRFLOW_PASSWORD)).json()
        writer.writerow({'dag_id':dag_dict['dag_id'],
              'is_paused':dag_dict['is_paused'],
              'is_subdag':dag_dict['is_subdag'],
              'is_active':dag_dict['is_active'],
              'fileloc':dag_dict['fileloc'],
              'file_token':dag_dict['file_token'],
              'owners':dag_dict['owners'],
              'description':dag_dict['description'],
                'root_dag_id': dag_dict['root_dag_id'],
              'schedule_interval':dag_dict['schedule_interval']})

