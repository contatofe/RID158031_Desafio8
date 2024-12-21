# Importações

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pyarrow.parquet as pq
from pyarrow import Table
import pandas as pd
import os

# Operadores

def upload_raw_data_to_bronze():

    print('initializing data extraction from raw folder')

    df = pd.read_csv('DNC_pipeline/RAW/raw_data.csv')

    print('saving data into bronze layer')

    pq.write_to_dataset(
        Table.from_pandas(df),
        root_path='DNC_pipeline/BRONZE'
    )



def process_bronze_to_silver():

    print('initializing data transformation from Bronze folder')

    df = pd.read_parquet('DNC_pipeline/BRONZE')

    print('removing null itens')

    df = df.dropna()

    print('correcting emails')

    df["email"].apply(
        lambda x: x.replace("example", "@example") if "example" in x and "@example" not in x else x
    )

    print('creating age column')

    df['date_of_birth'] = pd.to_datetime(df['date_of_birth'])
    df['age'] = ((datetime.now() - df['date_of_birth']).dt.days / 365.25).astype(int)

    print('saving data into silver layer')

    pq.write_to_dataset(
        Table.from_pandas(df),
        root_path='DNC_pipeline/SILVER'
    )


def process_silver_to_gold():

    print('initializing data transformation from Silver folder')

    df = pd.read_parquet('DNC_pipeline/SILVER')

    print('creating age groups')

    df_age = df['age'].apply(
        lambda x: '0 to 10' if x <= 10 else
                '11 to 20' if x <= 20 else
                '21 to 30' if x <= 30 else
                '31 to 40' if x <= 40 else
                '41 to 50' if x <= 50 else
                '51 to 60' if x <= 60 else
                '61 to 70' if x <= 70 else
                '71 to 80' if x <= 80 else
                '81 to 90' if x <= 90 else
                '91+'
    )

    print('creating table with age groups and subscription status')

    df_gold = pd.DataFrame({
        'age_groups': df_age,
        'subscription_status': df['subscription_status']
    })

    print('creating table grouping age groups by subscription status')

    counted_age = df_gold.groupby('age_groups')['subscription_status'].value_counts().unstack()

    print('saving data into golden layer')

    pq.write_to_dataset(
        Table.from_pandas(counted_age),
        root_path='DNC_pipeline/GOLD'
    )


# Configurações da DAG

default_args = {
    'owner': 'Contato',
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

with DAG(
    dag_id = "pipeline_dag",
    default_args = default_args,
    description = "DAG do desafio 8 - DNC",
    start_date = datetime(2024,1,1),
    schedule_interval = '@daily',
    catchup = False
) as dag:
    task_1 = PythonOperator(
        task_id = 'Extractor_Loader',
        python_callable = upload_raw_data_to_bronze
    )

    task_2 = PythonOperator(
        task_id = 'Process_Bronze_to_Silver',
        python_callable = process_bronze_to_silver
    )

    task_3 = PythonOperator(
        task_id = 'Process_Silver_to_Gold',
        python_callable = process_silver_to_gold
    )

    task_1 >> task_2 >> task_3