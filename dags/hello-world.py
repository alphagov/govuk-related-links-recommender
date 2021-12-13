from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
import logging
import os


import pymongo
import runpy
import subprocess


logger = logging.getLogger("airflow.task")
data_directory = 'data'


def run_command(command):
    return subprocess.check_output(command, shell=True)


def init_mongo():
    mongodb_uri = Variable.get('mongodb_uri')
    mongo_client = pymongo.MongoClient(mongodb_uri)
    content_store_db = mongo_client.get_default_database()
    content_store_collection = content_store_db['content_items']
    random_entry = content_store_collection.find_one()
    return f'This seems to have worked: {random_entry}'


def init():
    try:
        os.mkdir(data_directory)
    except FileExistsError:
        pass
    return 'init ok 2'


def end():
    return 'all done'


def make_structural_network(mongodb_uri):
    print('starting make_structural_network 4')

    import src.data_preprocessing.get_content_store_data as gcsd
    gcsd.run(mongodb_uri, data_directory)
    # runpy.run_path(
    #     'dags/src/data_preprocessing/get_content_store_data.py',
    #     run_name='__main__',
    #     init_globals={'mongodb_uri': mongodb_uri})
    return 'structural network built'


dag = DAG('hello_world', description='Hello World DAG',
          schedule_interval='0 12 * * *',
          start_date=datetime(2017, 3, 20), catchup=False)


# Operators

make_structural_network_task = PythonOperator(
    task_id='make_structural_network',
    python_callable=make_structural_network,
    op_kwargs={'mongodb_uri': Variable.get('mongodb_uri')},
    dag=dag)

init_task = PythonOperator(
    task_id='init_task',
    python_callable=init,
    dag=dag)

end_task = PythonOperator(
    task_id='end_task',
    python_callable=end,
    dag=dag)


# Start

init_task >> make_structural_network_task >> end_task
