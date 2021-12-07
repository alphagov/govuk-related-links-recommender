from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
import pymongo


def init_mongo():
    mongodb_uri = Variable.get('mongodb_uri')

    mongo_client = pymongo.MongoClient(mongodb_uri)
    content_store_db = mongo_client.get_default_database()
    content_store_collection = content_store_db['content_items']
    random_entry = content_store_collection.find_one()
    return f'This seems to have worked: {random_entry}'


def init():
    return init_mongo()


dag = DAG('hello_world', description='Hello World DAG',
          schedule_interval='0 12 * * *',
          start_date=datetime(2017, 3, 20), catchup=False)

hello_operator = PythonOperator(task_id='hello_task', python_callable=init, dag=dag)

hello_operator
