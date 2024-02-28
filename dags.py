from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.datasets import Dataset
from airflow.decorators import task_group
from airflow.providers.mongo.hooks.mongo import MongoHook
from datetime import datetime
import logging
import pandas as pd
import re
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


LOG_PATH = '/mnt/c/Users/User/PycharmProjects/task_7/logfile.log'
FILE_PATH = '/mnt/c/Users/User/PycharmProjects/task_7/work_folder/'
FILE_NAME = 'tiktok_google_play_reviews.csv'
FILE_TEMP_PATH = f'{FILE_PATH}tmp_{FILE_NAME}'
FILE_NEW_PATH = f'{FILE_PATH}new_{FILE_NAME}'

dataset = Dataset('file:/'+FILE_PATH+FILE_NAME)
new_dataset = Dataset('file:/'+FILE_NEW_PATH)

default_args = {
    'owner': 'samsdimko',
    'depends_on_past': False,
}

def check_file_emptiness(**context):
    task_instance = context['task_instance']
    task_instance.xcom_push('is_empty', os.stat(FILE_PATH+FILE_NAME).st_size == 0)


def branch_on_file_emptiness(**context):
    ti = context['ti']
    is_empty = ti.xcom_pull(key='is_empty', task_ids='check_file_emptiness_task')
    if is_empty:
        return "log_empty_file"
    else:
        return "file_transforming_group.replace_nulls"


def log_empty_file():
    logger.info('File is empty.')


def log_success_transform():
    logger.info(f'File is successfully transformed in {FILE_NEW_PATH}')


def replace_nulls():
    df = pd.read_csv(FILE_PATH + FILE_NAME)
    df.fillna('-', inplace=True)
    df = df.replace('', '-', regex=True)
    df.to_csv(FILE_TEMP_PATH, index=False)


def sort_by_date():
    df = pd.read_csv(FILE_TEMP_PATH)
    df = df.sort_values(by=['at'], ascending=False)
    df.to_csv(FILE_TEMP_PATH, index=False)


def delete_unnecessary_symbols(text):
    allowed_chars = re.compile(r"[^\w\s!\"\'()*,.:;?\-]")
    return allowed_chars.sub("", text)


def delete_unnecessary_symbols_from_data():
    df = pd.read_csv(FILE_TEMP_PATH)
    df['content'] = df['content'].apply(delete_unnecessary_symbols)
    df = df.replace('', '-', regex=True)
    df.fillna('-', inplace=True)
    df.to_csv(FILE_NEW_PATH, index=False)
    os.remove(FILE_TEMP_PATH)


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
    dag_id='data_file_sensor_dag',
    default_args=default_args,
    start_date=datetime.combine(datetime.now().date(), datetime.min.time()),
    schedule=None,
    catchup=False

) as dag1:

    data_file_sensor = FileSensor(
        task_id='wait_for_data_file',
        filepath=FILE_PATH+FILE_NAME,
        poke_interval=10,
    )
    
    file_is_empty_operator = PythonOperator(
        task_id='check_file_emptiness_task',
        python_callable=check_file_emptiness,
        provide_context=True,
    )

    branching = BranchPythonOperator(
        task_id='empty_branching',
        python_callable=branch_on_file_emptiness,
        provide_context=True,
    )

    log_empty = PythonOperator(
        task_id='log_empty_file',
        python_callable=log_empty_file,
    )

    @task_group(group_id='file_transforming_group')
    def transform_group():
        replace_nulls_in_file = PythonOperator(
            task_id='replace_nulls',
            python_callable=replace_nulls,
        )

        sort_data = PythonOperator(
            task_id='sort_data',
            python_callable=sort_by_date,
        )

        delete_symbols = PythonOperator(
            task_id='delete_symbols',
            python_callable=delete_unnecessary_symbols_from_data,
            outlets=[new_dataset]
        )

        replace_nulls_in_file >> sort_data >> delete_symbols

    log_success = PythonOperator(
        task_id='log_success',
        python_callable=log_success_transform,
        provide_context=True,
    )

    data_file_sensor >> file_is_empty_operator >> branching
    branching >> log_empty
    branching >> transform_group() >> log_success



with DAG(
    dag_id='data_upload_dag',
    default_args=default_args,
    schedule=[new_dataset],    
    start_date=datetime.combine(datetime.now().date(), datetime.min.time()),
    catchup=False

) as dag2:
    upload_csv_task = PythonOperator(
        task_id="upload_csv_to_mongo",
        python_callable=upload_csv_to_mongo,
        op_kwargs={
            "csv_filepath": FILE_NEW_PATH,
        }
    )

    log_success = PythonOperator(
        task_id='log_success',
        python_callable=log_success_upload,
        provide_context=True,
    )

    upload_csv_task >> log_success