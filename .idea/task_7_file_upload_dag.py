from airflow import DAG
from airflow.operators.python import PythonOperator 
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.datasets import Dataset
from airflow.providers.mongo.hooks.mongo import MongoHook
import pandas as pd
import datetime
import logging

FILE_PATH = '/mnt/c/Users/User/PycharmProjects/task_7/work_folder/'
FILE_NAME = 'tiktok_google_play_reviews.csv'
FILE_NEW_PATH = f'{FILE_PATH}new_{FILE_NAME}'

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

default_args = {
    'owner': 'samsdimko',
    'depends_on_past': False,
}


def upload_csv_to_mongo(csv_filepath):
    hook = MongoHook(mongo_conn_id='mongo_conn')
    client = hook.get_conn()
    db = client.reviews
    collection = db.tiktok

    df = pd.read_csv(csv_filepath)
    records = df.to_dict("records")
    collection.insert_many(records)

    client.close()


def log_success_upload():
    logger.info('File is successfully uploaded')


with DAG(
    dag_id='data_upload_dag',
    default_args=default_args,
    schedule=[Dataset('file://'+FILE_PATH+FILE_NAME)],
    start_date=datetime.datetime.now() - datetime.timedelta(days=1),
    catchup=False

) as dag:
    upload_csv_task = PythonOperator(
        task_id="upload_csv_to_mongo",
        python_callable=upload_csv_to_mongo,
        op_kwargs={
            "csv_filepath": f'{FILE_PATH}{FILE_NAME}',
        }
    )

    log_success = PythonOperator(
        task_id='log_success',
        python_callable=log_success_upload,
        provide_context=True,
    )

    upload_csv_task >> log_success